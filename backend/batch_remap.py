"""批重映（甲路径：强制游标修复版）。

修复口径见 ops/跳号修复说明.md（2026-11-20 锁定，当天禁止改口）：
  甲 续跑必须携带上次 cursor，缺失/不符整笔拒绝（400），正式零增、cursor 不动；
  新 cursor 取本窗实际末处理行 id，禁止算术累加；
  单窗单事务，失败整体回滚；
  两路同 batch 推进 cursor 须占批锁，后到只记争用（409）；
  完成旗以 processed 计数=应收键数为准；
  cursor 账、正式实体、skip 审计、批回执、互证页五处同态。
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime

from fastapi import HTTPException
from pydantic import BaseModel

# 测试用故障注入：{batch_id: "fault_on:"+old_key}，命中即抛 RuntimeError（事务回滚）。
FAULTS: dict[str, str] = {}

# 测试用阶段钩子：{stage: callable()}，用于并发测把两路线程卡在同一阶段。
HOOKS: dict[str, object] = {}


def _hook(stage: str) -> None:
    fn = HOOKS.get(stage)
    if callable(fn):
        fn()


BATCH_SCHEMA = """
CREATE TABLE IF NOT EXISTS remap_batches(
  batch_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  total_keys INTEGER NOT NULL,
  limit_size INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  finished_at TEXT
);
CREATE TABLE IF NOT EXISTS inbound_keys(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL,
  old_key TEXT NOT NULL,
  new_key TEXT NOT NULL,
  processed INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS cursor_ledger(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL,
  last_cursor INTEGER NOT NULL,
  written_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS skip_audit(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL,
  old_key TEXT NOT NULL,
  reason TEXT NOT NULL,
  written_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batch_receipts(
  batch_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  ledger_rows INTEGER NOT NULL,
  formal_rows INTEGER NOT NULL,
  skip_rows INTEGER NOT NULL,
  expected_keys INTEGER NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batch_locks(
  batch_id TEXT PRIMARY KEY,
  holder TEXT NOT NULL,
  acquired_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS lock_events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL,
  event TEXT NOT NULL,
  holder TEXT NOT NULL,
  detail TEXT NOT NULL,
  written_at TEXT NOT NULL
);
"""


def ensure_batch_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(BATCH_SCHEMA)
    has_formal = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='formal_entities'"
    ).fetchone()
    if has_formal:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(formal_entities)").fetchall()}
        if "batch_id" not in cols:
            conn.execute("ALTER TABLE formal_entities ADD COLUMN batch_id TEXT")


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


class BatchSeedIn(BaseModel):
    batch_id: str
    limit: int = 3
    items: list[dict]


class BatchRemapIn(BaseModel):
    batch_id: str
    resume_cursor: int | None = None


def seed_batch(conn: sqlite3.Connection, body: BatchSeedIn) -> None:
    ensure_batch_tables(conn)
    if conn.execute("SELECT 1 FROM remap_batches WHERE batch_id=?", (body.batch_id,)).fetchone():
        raise HTTPException(409, "批次已存在")
    conn.execute(
        "INSERT INTO remap_batches(batch_id,status,total_keys,limit_size,created_at)"
        " VALUES (?,?,?,?,?)",
        (body.batch_id, "进行中", len(body.items), body.limit, _now()),
    )
    conn.executemany(
        "INSERT INTO inbound_keys(batch_id,old_key,new_key) VALUES (?,?,?)",
        [(body.batch_id, it["old_key"], it["new_key"]) for it in body.items],
    )
    conn.execute(
        "INSERT INTO cursor_ledger(batch_id,last_cursor,written_at) VALUES (?,?,?)",
        (body.batch_id, 0, _now()),
    )
    conn.commit()


def _last_cursor(conn: sqlite3.Connection, batch_id: str) -> int:
    row = conn.execute(
        "SELECT last_cursor FROM cursor_ledger WHERE batch_id=? ORDER BY id DESC LIMIT 1",
        (batch_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(404, "批次不存在")
    return row["last_cursor"]


def run_batch_remap(conn: sqlite3.Connection, body: BatchRemapIn) -> dict:
    """甲路径：强制 cursor + 批锁 + 单事务真实游标。"""
    ensure_batch_tables(conn)
    batch = conn.execute(
        "SELECT * FROM remap_batches WHERE batch_id=?", (body.batch_id,)
    ).fetchone()
    if batch is None:
        raise HTTPException(404, "批次不存在")

    # 甲-1：续跑必须携带上次 cursor，缺则整笔拒绝。
    if body.resume_cursor is None:
        raise HTTPException(400, "续跑必须携带上次 cursor；缺失已整笔拒绝，正式零增")

    holder = uuid.uuid4().hex
    receipt: dict | None = None
    actual_last_id = body.resume_cursor if body.resume_cursor is not None else 0
    formal_written = 0
    # 占锁与争用留痕走自动提交短事务，使 busy_timeout 对后来者生效，
    # 争用确定性地落在 batch_locks 主键冲突上（而非文件锁超时）。
    try:
        _hook("before_lock_acquire")
        try:
            conn.execute(
                "INSERT INTO batch_locks(batch_id,holder,acquired_at) VALUES (?,?,?)",
                (body.batch_id, holder, _now()),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            conn.rollback()
            # 败者：此时胜者只持应用批锁、不持 SQLite 写锁，留痕写入无阻塞。
            conn.execute(
                "INSERT INTO lock_events(batch_id,event,holder,detail,written_at) VALUES (?,?,?,?,?)",
                (body.batch_id, "争用拒绝", holder, "批锁被占，后到者争用拒绝", _now()),
            )
            conn.commit()
            raise HTTPException(409, "批锁争用：另一路正在推进该 batch，已记争用")

        # 胜者钩子（败者不会到达）：测试可在此让胜者停留，留出确定性争用窗口。
        _hook("winner_after_lock")

        # 业务单事务收口：cursor 校验、ledger、正式实体、完成旗、回执同生共死。
        conn.execute("BEGIN")
        try:
            expected_cursor = _last_cursor(conn, body.batch_id)

            # 甲-1：cursor 不符整笔拒绝（回滚，正式零增、账不动、锁不留）。
            # None 已在占锁前判过；此处再校验账上末值，防错带/重放。
            if body.resume_cursor != expected_cursor:
                raise HTTPException(
                    400,
                    f"cursor 不符：携带 {body.resume_cursor}，账上末值 {expected_cursor}；整笔拒绝",
                )

            rows = conn.execute(
                "SELECT * FROM inbound_keys"
                " WHERE batch_id=? AND id>? AND processed=0 ORDER BY id LIMIT ?",
                (body.batch_id, expected_cursor, batch["limit_size"]),
            ).fetchall()

            _hook("after_cursor_read")
            _hook("before_ledger")

            formal_written = 0
            actual_last_id = expected_cursor
            for r in rows:
                fault = FAULTS.get(body.batch_id)
                if fault and fault == "fault_on:" + r["old_key"]:
                    # 单事务：抛错即整体回滚，cursor 与正式增量都不前进。
                    raise RuntimeError("注入故障：处理中断于 " + r["old_key"])
                conn.execute(
                    "INSERT INTO formal_entities"
                    "(entity_key,mapped_from,version,created_at,batch_id)"
                    " VALUES (?,?,?,?,?)",
                    (r["new_key"], r["old_key"], "m-2026.11", _now(), body.batch_id),
                )
                conn.execute(
                    "UPDATE inbound_keys SET processed=1 WHERE id=?", (r["id"],)
                )
                formal_written += 1
                actual_last_id = r["id"]

            # 甲-3：真实游标，取实际末处理行 id；无处理窗则 cursor 不动。
            if rows:
                conn.execute(
                    "INSERT INTO cursor_ledger(batch_id,last_cursor,written_at) VALUES (?,?,?)",
                    (body.batch_id, actual_last_id, _now()),
                )

            # 甲-5：完成旗只认 processed 计数=应收键数，窗口取空不再是依据。
            processed_n = conn.execute(
                "SELECT COUNT(*) c FROM inbound_keys WHERE batch_id=? AND processed=1",
                (body.batch_id,),
            ).fetchone()["c"]
            if processed_n >= batch["total_keys"]:
                conn.execute(
                    "UPDATE remap_batches SET status='已完成', finished_at=? WHERE batch_id=?",
                    (_now(), body.batch_id),
                )

            conn.execute(
                "INSERT INTO lock_events(batch_id,event,holder,detail,written_at) VALUES (?,?,?,?,?)",
                (body.batch_id, "占锁推进", holder, f"本窗正式写入 {formal_written} 键", _now()),
            )

            receipt = _build_receipt(conn, body.batch_id, commit=False)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        # 甲-批锁：任何路径都清理本持有者可能残留的锁，不留死锁。
        try:
            conn.execute(
                "DELETE FROM batch_locks WHERE batch_id=? AND holder=?",
                (body.batch_id, holder),
            )
            conn.commit()
        except sqlite3.Error:
            pass

    return {
        "last_cursor": actual_last_id,
        "formal_written": formal_written,
        "receipt": receipt,
    }


def _build_receipt(conn: sqlite3.Connection, batch_id: str, commit: bool = True) -> dict:
    batch = conn.execute(
        "SELECT * FROM remap_batches WHERE batch_id=?", (batch_id,)
    ).fetchone()
    ledger_rows = max(
        conn.execute(
            "SELECT COUNT(*) c FROM cursor_ledger WHERE batch_id=?", (batch_id,)
        ).fetchone()["c"]
        - 1,
        0,
    )
    formal_rows = conn.execute(
        "SELECT COUNT(*) c FROM formal_entities WHERE batch_id=?", (batch_id,)
    ).fetchone()["c"]
    skip_rows = conn.execute(
        "SELECT COUNT(*) c FROM skip_audit WHERE batch_id=?", (batch_id,)
    ).fetchone()["c"]
    receipt = {
        "batch_id": batch_id,
        "status": batch["status"],
        "ledger_rows": ledger_rows,
        "formal_rows": formal_rows,
        "skip_rows": skip_rows,
        "expected_keys": batch["total_keys"],
        "created_at": _now(),
    }
    conn.execute(
        "INSERT OR REPLACE INTO batch_receipts"
        "(batch_id,status,ledger_rows,formal_rows,skip_rows,expected_keys,created_at)"
        " VALUES (:batch_id,:status,:ledger_rows,:formal_rows,:skip_rows,:expected_keys,:created_at)",
        receipt,
    )
    if commit:
        conn.commit()
    return receipt
