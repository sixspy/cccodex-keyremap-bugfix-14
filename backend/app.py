
import hashlib, secrets, sqlite3
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

def connect():
    """返回 (conn, writable)。只读库时 writable=False，读端点照常、写端点显式拒绝。"""
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=3000")
    writable = True
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except sqlite3.OperationalError:
        writable = False
    if writable:
        import batch_remap
        try:
            batch_remap.ensure_batch_tables(conn)
        except sqlite3.OperationalError:
            writable = False
    return conn, writable


def require_writable(writable: bool):
    if not writable:
        raise HTTPException(503, "数据库只读，批重映写操作不可用")

def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))

class LoginIn(BaseModel):
    username: str
    password: str

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u

from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "实体键桥"
SKELETON = "实体键映射生效后入站先重映再进正式区，冲突进队列，映射版本与正式区对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def init_db():
    conn, writable = connect()
    if not writable:
        conn.close()
        return
    with conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS map_versions(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT UNIQUE NOT NULL,
          status TEXT NOT NULL,
          effective_from TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS mappings(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT NOT NULL,
          old_key TEXT NOT NULL,
          new_key TEXT NOT NULL,
          kind TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS formal_entities(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          entity_key TEXT NOT NULL,
          mapped_from TEXT NOT NULL,
          version TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS conflicts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          old_key TEXT NOT NULL,
          reason TEXT NOT NULL,
          status TEXT NOT NULL
        );
        """)
        import batch_remap
        batch_remap.ensure_batch_tables(conn)
        if conn.execute("SELECT COUNT(*) c FROM map_versions").fetchone()["c"] == 0:
            conn.execute("INSERT INTO map_versions(version,status,effective_from) VALUES (?,?,?)", ("m-2026.11","生效","2026-11-01"))
            conn.executemany("INSERT INTO mappings(version,old_key,new_key,kind) VALUES (?,?,?,?)", [
                ("m-2026.11","OLD-A1","NEW-100","一对一"),
                ("m-2026.11","OLD-B1","NEW-200","多对一"),
                ("m-2026.11","OLD-B2","NEW-200","多对一"),
            ])
            conn.execute("INSERT INTO formal_entities(entity_key,mapped_from,version,created_at) VALUES (?,?,?,?)",
                         ("NEW-100","OLD-A1","m-2026.11","2026-11-10T09:00:00"))
            conn.execute("INSERT INTO conflicts(old_key,reason,status) VALUES (?,?,?)", ("OLD-C9","无映射","挂起"))

@app.on_event("startup")
def _s(): init_db()

@app.get("/api/health")
def health():
    c, _writable = connect()
    with c:
        n = c.execute("SELECT COUNT(*) c FROM mappings").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "mappings": n, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    c, _writable = connect()
    with c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row: raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok]=row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/map-versions")
def versions(authorization: str | None = Header(default=None)):
    require_user(authorization)
    c, _writable = connect()
    with c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM map_versions ORDER BY id").fetchall()]}

@app.get("/api/mappings")
def mappings(authorization: str | None = Header(default=None)):
    require_user(authorization)
    c, _writable = connect()
    with c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM mappings ORDER BY id").fetchall()]}

class RemapIn(BaseModel):
    old_key: str

@app.post("/api/remap")
def remap(body: RemapIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    now = datetime.now().isoformat(timespec="seconds")
    c, writable = connect()
    require_writable(writable)
    with c:
        ver = c.execute("SELECT version FROM map_versions WHERE status='生效' ORDER BY id DESC LIMIT 1").fetchone()
        if not ver: raise HTTPException(404, "无生效映射版本")
        m = c.execute("SELECT * FROM mappings WHERE version=? AND old_key=?", (ver["version"], body.old_key)).fetchone()
        if not m:
            c.execute("INSERT INTO conflicts(old_key,reason,status) VALUES (?,?,?)", (body.old_key, "无映射", "挂起"))
            c.commit()
            raise HTTPException(409, "无映射，已进冲突队列")
        c.execute("INSERT INTO formal_entities(entity_key,mapped_from,version,created_at) VALUES (?,?,?,?)",
                  (m["new_key"], body.old_key, ver["version"], now))
        return {"entity_key": m["new_key"], "version": ver["version"]}

@app.get("/api/formal")
def formal(authorization: str | None = Header(default=None)):
    require_user(authorization)
    c, _writable = connect()
    with c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM formal_entities ORDER BY id DESC").fetchall()]}

@app.get("/api/conflicts")
def conflicts(authorization: str | None = Header(default=None)):
    require_user(authorization)
    c, _writable = connect()
    with c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM conflicts ORDER BY id DESC").fetchall()]}

import batch_remap

@app.post("/api/batch/seed")
def batch_seed(body: batch_remap.BatchSeedIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    c, writable = connect()
    require_writable(writable)
    with c:
        batch_remap.seed_batch(c, body)
        return {"batch_id": body.batch_id, "total_keys": len(body.items), "limit": body.limit}

@app.post("/api/batch/remap")
def batch_remap_run(body: batch_remap.BatchRemapIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    c, writable = connect()
    require_writable(writable)
    with c:
        try:
            return batch_remap.run_batch_remap(c, body)
        except HTTPException:
            raise
        except RuntimeError as e:
            raise HTTPException(500, str(e))

@app.get("/api/batch/cursor")
def batch_cursor(batch_id: str, authorization: str | None = Header(default=None)):
    require_user(authorization)
    c, writable = connect()
    with c:
        if writable:
            batch_remap.ensure_batch_tables(c)
        row = c.execute(
            "SELECT * FROM cursor_ledger WHERE batch_id=? ORDER BY id DESC LIMIT 1", (batch_id,)
        ).fetchone()
        if not row: raise HTTPException(404, "批次不存在")
        history = [dict(r) for r in c.execute(
            "SELECT id,last_cursor,written_at FROM cursor_ledger WHERE batch_id=? ORDER BY id",
            (batch_id,)).fetchall()]
        return {"batch_id": batch_id, "last_cursor": row["last_cursor"], "history": history}

@app.get("/api/batch/receipt")
def batch_receipt(batch_id: str, authorization: str | None = Header(default=None)):
    require_user(authorization)
    c, writable = connect()
    with c:
        if writable:
            batch_remap.ensure_batch_tables(c)
        row = c.execute("SELECT * FROM batch_receipts WHERE batch_id=?", (batch_id,)).fetchone()
        if not row: raise HTTPException(404, "回执不存在")
        return dict(row)

@app.get("/api/batch/proof")
def batch_proof(batch_id: str, authorization: str | None = Header(default=None)):
    require_user(authorization)
    c, writable = connect()
    with c:
        if writable:
            batch_remap.ensure_batch_tables(c)
        b = c.execute("SELECT * FROM remap_batches WHERE batch_id=?", (batch_id,)).fetchone()
        if not b: raise HTTPException(404, "批次不存在")
        pending = c.execute(
            "SELECT COUNT(*) c FROM inbound_keys WHERE batch_id=? AND processed=0", (batch_id,)
        ).fetchone()["c"]
        processed = c.execute(
            "SELECT COUNT(*) c FROM inbound_keys WHERE batch_id=? AND processed=1", (batch_id,)
        ).fetchone()["c"]
        last_processed_id = c.execute(
            "SELECT MAX(id) m FROM inbound_keys WHERE batch_id=? AND processed=1", (batch_id,)
        ).fetchone()["m"] or 0
        formal_rows = c.execute(
            "SELECT COUNT(*) c FROM formal_entities WHERE batch_id=?", (batch_id,)
        ).fetchone()["c"]
        skip_rows = c.execute(
            "SELECT COUNT(*) c FROM skip_audit WHERE batch_id=?", (batch_id,)
        ).fetchone()["c"]
        ledger_max = c.execute(
            "SELECT MAX(last_cursor) m FROM cursor_ledger WHERE batch_id=?", (batch_id,)
        ).fetchone()["m"] or 0
        receipt = c.execute(
            "SELECT * FROM batch_receipts WHERE batch_id=?", (batch_id,)
        ).fetchone()
        contention = c.execute(
            "SELECT COUNT(*) c FROM lock_events WHERE batch_id=? AND event='争用拒绝'", (batch_id,)
        ).fetchone()["c"]
        locks_held = c.execute(
            "SELECT COUNT(*) c FROM batch_locks WHERE batch_id=?", (batch_id,)
        ).fetchone()["c"]

        # 五处同态：cursor 账 / 正式实体 / skip 审计 / 批回执 / 互证页。
        checks = {
            "cursor_matches_last_row": ledger_max == last_processed_id,
            "formal_matches_processed": formal_rows == processed,
            "skip_is_zero": skip_rows == 0,
            "receipt_matches": (
                receipt is not None
                and receipt["formal_rows"] == formal_rows
                and receipt["skip_rows"] == skip_rows
                and receipt["expected_keys"] == b["total_keys"]
            ),
            "no_pending_when_done": b["status"] != "已完成" or pending == 0,
        }
        homomorphic = all(checks.values())
        return {
            "batch_id": batch_id,
            "status": b["status"],
            "expected_keys": b["total_keys"],
            "pending_keys": pending,
            "processed_keys": processed,
            "formal_rows": formal_rows,
            "skip_rows": skip_rows,
            "ledger_max_cursor": ledger_max,
            "last_processed_row_id": last_processed_id,
            "contention_events": contention,
            "locks_held": locks_held,
            "homomorphic": homomorphic,
            "checks": checks,
            "receipt": dict(receipt) if receipt else None,
        }

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")
app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
