"""跳号修复红测（先红后绿，禁止跳过）。

纪律：
  1. test_cursor_skip_false_completion 先复现“漏映却完成绿、正式增量对不上应收”。
  2. test_batch_lock_contention 再复现“两路同 batch 抢写无锁、后到不记争用”。
两条红测见绿之前，不得改写游标写路径。
路径（甲：强制 cursor / 乙：全量幂等）择定写入 ops/跳号修复说明.md，当天锁定。
本测最终按甲路径断言。
"""

import json
import sqlite3
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import app, init_db
import batch_remap

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = json.loads((ROOT / "fixtures" / "inbound_batch.json").read_text(encoding="utf-8"))


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    batch_remap.FAULTS.clear()
    batch_remap.HOOKS.clear()
    init_db()
    yield
    batch_remap.FAULTS.clear()
    batch_remap.HOOKS.clear()


@pytest.fixture()
def auth():
    c = TestClient(app)
    tok = c.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    return c, {"Authorization": "Bearer " + tok}


def _seed(client, headers, batch_id="B-LOCAL-001", limit=3):
    body = {"batch_id": batch_id, "limit": limit, "items": FIXTURE["items"]}
    r = client.post("/api/batch/seed", headers=headers, json=body)
    assert r.status_code == 200, r.text
    return body


def _raw_conn():
    import app as app_mod
    conn = sqlite3.connect(app_mod.DB_PATH, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=3000")
    batch_remap.ensure_batch_tables(conn)
    return conn


def test_cursor_skip_false_completion(auth):
    """复现事故：中断后续跑 cursor 跳号，中间旧键未映，批次却绿，增量对不上应收。"""
    client, headers = auth
    _seed(client, headers)

    # 第 1 窗：id 1..3 正常（甲路径首批显式携带 cursor=0）。
    r1 = client.post(
        "/api/batch/remap",
        headers=headers,
        json={"batch_id": "B-LOCAL-001", "resume_cursor": 0},
    )
    assert r1.status_code == 200, r1.text
    assert r1.json()["last_cursor"] == 3

    # 第 2 窗在处理 OLD-K04 时中断；甲路径单事务回滚，cursor 必须停在 3。
    batch_remap.FAULTS["B-LOCAL-001"] = "fault_on:OLD-K04"
    r2 = client.post(
        "/api/batch/remap",
        headers=headers,
        json={"batch_id": "B-LOCAL-001", "resume_cursor": 3},
    )
    assert r2.status_code == 500, r2.text
    batch_remap.FAULTS.clear()

    # 续跑：从账上末值 3 继续，直到窗口取空。
    last = 3
    for _ in range(4):
        rr = client.post(
            "/api/batch/remap",
            headers=headers,
            json={"batch_id": "B-LOCAL-001", "resume_cursor": last},
        )
        if rr.status_code == 200:
            last = rr.json()["last_cursor"]

    with _raw_conn() as conn:
        status = conn.execute(
            "SELECT status FROM remap_batches WHERE batch_id='B-LOCAL-001'"
        ).fetchone()["status"]
        unprocessed = [
            r["old_key"]
            for r in conn.execute(
                "SELECT old_key FROM inbound_keys WHERE batch_id='B-LOCAL-001' AND processed=0 ORDER BY id"
            ).fetchall()
        ]
        formal_n = conn.execute(
            "SELECT COUNT(*) c FROM formal_entities WHERE batch_id='B-LOCAL-001'"
        ).fetchone()["c"]
        expected = conn.execute(
            "SELECT total_keys c FROM remap_batches WHERE batch_id='B-LOCAL-001'"
        ).fetchone()["c"]

    # 核心断言一：不得假完成——有未映键时状态必须不是“已完成”。
    assert unprocessed == [], f"存在漏映旧键 {unprocessed}，cursor 发生跳号"
    assert status != "已完成" or not unprocessed, "漏映却批完成旗已绿（假完成）"
    # 核心断言二：正式增量必须等于应收键数。
    assert formal_n == expected, f"正式增量 {formal_n} 与应收键数 {expected} 对不上"


def test_batch_lock_contention(auth):
    """两路同 batch 同时推进 cursor：须占批锁，后到只记争用，不得双写。"""
    client, headers = auth
    _seed(client, headers)

    # 占锁成功者（胜者，无论哪个线程）在钩子里停留，持应用批锁、不持 SQLite 写锁；
    # 撞锁者（败者）不进钩子，直接主键冲突并写争用事件。角色由调度决定，钩子动态生效。
    winner_release = threading.Event()
    loser_done = threading.Event()

    def winner_hook():
        # 等到败者完成争用留痕，再放行胜者，争用窗口确定。
        loser_done.wait(timeout=10)
        winner_release.wait(timeout=10)

    batch_remap.HOOKS["winner_after_lock"] = winner_hook

    results: list[dict] = []

    def worker(idx: int):
        c = TestClient(app)
        try:
            r = c.post(
                "/api/batch/remap",
                headers=headers,
                json={"batch_id": "B-LOCAL-001", "resume_cursor": 0},
            )
            results.append({"idx": idx, "status": r.status_code, "body": r.json()})
        finally:
            # 撞锁返回的那一路（败者）触发事件，放行可能正停留的胜者。
            loser_done.set()

    threads = [threading.Thread(target=worker, args=(i,)) for i in (0, 1)]
    for t in threads:
        t.start()
    assert loser_done.wait(timeout=10), "败者未在限时内完成争用"
    winner_release.set()
    for t in threads:
        t.join(timeout=15)

    with _raw_conn() as conn:
        formal_n = conn.execute(
            "SELECT COUNT(*) c FROM formal_entities WHERE batch_id='B-LOCAL-001'"
        ).fetchone()["c"]
        distinct = conn.execute(
            "SELECT COUNT(DISTINCT mapped_from) c FROM formal_entities WHERE batch_id='B-LOCAL-001'"
        ).fetchone()["c"]
        contention = conn.execute(
            "SELECT COUNT(*) c FROM lock_events WHERE batch_id='B-LOCAL-001' AND event='争用拒绝'"
        ).fetchone()["c"]
        lock_left = conn.execute(
            "SELECT COUNT(*) c FROM batch_locks WHERE batch_id='B-LOCAL-001'"
        ).fetchone()["c"]

    statuses = sorted(x["status"] for x in results)
    # 一路放行、一路争用拒绝；不得两路同写。
    assert statuses == [200, 409], f"争用语义错误，实际状态 {statuses}"
    assert contention >= 1, "后到者未记争用"
    assert formal_n == distinct == 3, f"发生双写 formal={formal_n} distinct={distinct}"
    assert lock_left == 0, "批锁未释放"


def test_resume_requires_last_cursor(auth):
    """甲路径：续跑必须携带上次 cursor，缺则整笔拒绝且正式零增。"""
    client, headers = auth
    _seed(client, headers)
    first = client.post(
        "/api/batch/remap",
        headers=headers,
        json={"batch_id": "B-LOCAL-001", "resume_cursor": 0},
    )
    assert first.status_code == 200, first.text
    last = first.json()["last_cursor"]

    before = client.get("/api/batch/proof?batch_id=B-LOCAL-001", headers=headers).json()
    bad = client.post(
        "/api/batch/remap", headers=headers, json={"batch_id": "B-LOCAL-001"}
    )
    after = client.get("/api/batch/proof?batch_id=B-LOCAL-001", headers=headers).json()

    assert bad.status_code == 400, bad.text
    assert "cursor" in bad.json()["detail"]
    # 整笔拒绝：cursor 账与正式实体零变化。
    assert after["formal_rows"] == before["formal_rows"]
    assert after["ledger_max_cursor"] == before["ledger_max_cursor"] == last

    # 携带正确 cursor 可续跑；错带 cursor 同样拒绝。
    wrong = client.post(
        "/api/batch/remap",
        headers=headers,
        json={"batch_id": "B-LOCAL-001", "resume_cursor": last - 1},
    )
    assert wrong.status_code == 400, wrong.text
    ok = client.post(
        "/api/batch/remap",
        headers=headers,
        json={"batch_id": "B-LOCAL-001", "resume_cursor": last},
    )
    assert ok.status_code == 200, ok.text


def test_full_run_no_missing_and_proof_consistent(auth):
    """全绿口径：跑完无漏键无 skip，五处同态，回执与互证页一致。"""
    client, headers = auth
    _seed(client, headers)
    last = 0
    for _ in range(6):
        r = client.post(
            "/api/batch/remap",
            headers=headers,
            json={"batch_id": "B-LOCAL-001", "resume_cursor": last},
        )
        if r.status_code == 200:
            last = r.json()["last_cursor"]

    proof = client.get("/api/batch/proof?batch_id=B-LOCAL-001", headers=headers).json()
    receipt = client.get("/api/batch/receipt?batch_id=B-LOCAL-001", headers=headers).json()
    cursor = client.get("/api/batch/cursor?batch_id=B-LOCAL-001", headers=headers).json()

    with _raw_conn() as conn:
        unprocessed = conn.execute(
            "SELECT COUNT(*) c FROM inbound_keys WHERE batch_id='B-LOCAL-001' AND processed=0"
        ).fetchone()["c"]
        last_processed_id = conn.execute(
            "SELECT MAX(id) m FROM inbound_keys WHERE batch_id='B-LOCAL-001' AND processed=1"
        ).fetchone()["m"]

    # 先证不漏：零未映、零 skip、正式增量等于应收，完成旗才允许绿。
    assert unprocessed == 0, f"漏映 {unprocessed} 键却可能已假完成"
    assert proof["formal_rows"] == proof["expected_keys"] == 10, "正式增量与应收键数对不上"
    assert proof["pending_keys"] == 0
    assert proof["skip_rows"] == 0
    assert proof["status"] == "已完成", "无漏键后才允许完成旗绿"
    # 五处同态：cursor 账、正式实体、skip 审计、批回执、互证页。
    # cursor 账末值必须等于实际最后处理行 id，不得是算术累加的跳号值。
    assert cursor["last_cursor"] == last_processed_id, (
        f"cursor 账 {cursor['last_cursor']} 与实际末处理行 {last_processed_id} 不同态"
    )
    assert proof["ledger_max_cursor"] == last_processed_id
    assert receipt["formal_rows"] == proof["formal_rows"]
    assert receipt["skip_rows"] == proof["skip_rows"] == 0
    assert receipt["expected_keys"] == proof["expected_keys"]


def test_fault_rolls_back_cursor_and_entities(auth):
    """处理失败整笔回滚：cursor 账与正式增量都不得前进。"""
    client, headers = auth
    _seed(client, headers)
    first = client.post(
        "/api/batch/remap",
        headers=headers,
        json={"batch_id": "B-LOCAL-001", "resume_cursor": 0},
    )
    last = first.json()["last_cursor"]

    batch_remap.FAULTS["B-LOCAL-001"] = "fault_on:OLD-K04"
    bad = client.post(
        "/api/batch/remap",
        headers=headers,
        json={"batch_id": "B-LOCAL-001", "resume_cursor": last},
    )
    batch_remap.FAULTS.clear()
    assert bad.status_code == 500, bad.text

    proof = client.get("/api/batch/proof?batch_id=B-LOCAL-001", headers=headers).json()
    assert proof["ledger_max_cursor"] == last, "失败后 cursor 仍前进"
    assert proof["formal_rows"] == 3, "失败后正式实体仍增长"
    assert proof["status"] == "进行中"
