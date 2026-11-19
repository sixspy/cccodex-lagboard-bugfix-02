
import hashlib, secrets, sqlite3, threading
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

# 半成切断说明(docs/半成切断说明.md,2026-11-19 封存):乙路径·外键同批绑定
CUT_PATH = "乙"
CUT_MECHANISM = "外键同批绑定"
CUT_INACTIVE = "预查拒写"
CUT_SEALED_ON = "2026-11-19"
CUT_DOC = "docs/半成切断说明.md"

def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

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

PRODUCT = "源到达SLA"
SKELETON = "多源到达时钟对照 SLA 分级，超限则冻结或降级输出，补齐后恢复并对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

SCHEMA = """
CREATE TABLE IF NOT EXISTS sources(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL,
  sla_minutes INTEGER NOT NULL,
  output_mode TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batches(
  batch_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  source TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS arrivals(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL REFERENCES batches(batch_id),
  source TEXT NOT NULL,
  source_clock TEXT NOT NULL,
  ingest_clock TEXT NOT NULL,
  lag_minutes INTEGER NOT NULL,
  level TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS source_states(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL REFERENCES batches(batch_id),
  source TEXT NOT NULL,
  output_mode TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS catchups(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL REFERENCES batches(batch_id),
  source TEXT NOT NULL,
  status TEXT NOT NULL,
  restored_mode TEXT NOT NULL,
  note TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS half_success_events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT NOT NULL,
  source TEXT NOT NULL,
  detail TEXT NOT NULL,
  created_at TEXT NOT NULL
);
"""

TRIGGERS = """
CREATE TRIGGER IF NOT EXISTS arrivals_bound_batch
BEFORE INSERT ON arrivals
WHEN NEW.batch_id IS NULL OR NOT EXISTS(SELECT 1 FROM batches b WHERE b.batch_id=NEW.batch_id)
BEGIN SELECT RAISE(ABORT, '半成偏了:到达须同批事务号外键绑定'); END;
CREATE TRIGGER IF NOT EXISTS source_states_bound_batch
BEFORE INSERT ON source_states
WHEN NEW.batch_id IS NULL OR NOT EXISTS(SELECT 1 FROM batches b WHERE b.batch_id=NEW.batch_id)
BEGIN SELECT RAISE(ABORT, '半成偏了:源态须同批事务号外键绑定'); END;
CREATE TRIGGER IF NOT EXISTS catchups_bound_batch
BEFORE INSERT ON catchups
WHEN NEW.batch_id IS NULL OR NOT EXISTS(SELECT 1 FROM batches b WHERE b.batch_id=NEW.batch_id)
BEGIN SELECT RAISE(ABORT, '半成偏了:补齐须同批事务号外键绑定'); END;
"""

def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")

def _migrate(conn):
    """旧库补齐同批绑定:到达/补齐重建带 batch_id,源态账册按现目录回填。"""
    now = _now()
    if "batch_id" not in [r["name"] for r in conn.execute("PRAGMA table_info(arrivals)")]:
        conn.execute("""CREATE TABLE arrivals_new(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id TEXT NOT NULL REFERENCES batches(batch_id),
          source TEXT NOT NULL, source_clock TEXT NOT NULL, ingest_clock TEXT NOT NULL,
          lag_minutes INTEGER NOT NULL, level TEXT NOT NULL)""")
        for r in conn.execute("SELECT * FROM arrivals ORDER BY id").fetchall():
            bid = f"B-LEGACY-A{r['id']}"
            conn.execute("INSERT INTO batches VALUES (?,?,?,?)", (bid, "legacy", r["source"], now))
            conn.execute("INSERT INTO arrivals_new(id,batch_id,source,source_clock,ingest_clock,lag_minutes,level) VALUES (?,?,?,?,?,?,?)",
                         (r["id"], bid, r["source"], r["source_clock"], r["ingest_clock"], r["lag_minutes"], r["level"]))
        conn.execute("DROP TABLE arrivals")
        conn.execute("ALTER TABLE arrivals_new RENAME TO arrivals")
    if "batch_id" not in [r["name"] for r in conn.execute("PRAGMA table_info(catchups)")]:
        conn.execute("""CREATE TABLE catchups_new(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id TEXT NOT NULL REFERENCES batches(batch_id),
          source TEXT NOT NULL, status TEXT NOT NULL, restored_mode TEXT NOT NULL,
          note TEXT NOT NULL, created_at TEXT NOT NULL)""")
        for r in conn.execute("SELECT * FROM catchups ORDER BY id").fetchall():
            bid = f"B-LEGACY-C{r['id']}"
            conn.execute("INSERT INTO batches VALUES (?,?,?,?)", (bid, "legacy", r["source"], now))
            conn.execute("INSERT INTO catchups_new(id,batch_id,source,status,restored_mode,note,created_at) VALUES (?,?,?,?,?,?,?)",
                         (r["id"], bid, r["source"], r["status"], r["restored_mode"], r["note"], r["created_at"]))
        conn.execute("DROP TABLE catchups")
        conn.execute("ALTER TABLE catchups_new RENAME TO catchups")
    if conn.execute("SELECT COUNT(*) c FROM source_states").fetchone()["c"] == 0:
        for s in conn.execute("SELECT * FROM sources ORDER BY id").fetchall():
            bid = f"B-LEGACY-S{s['id']}"
            conn.execute("INSERT OR IGNORE INTO batches VALUES (?,?,?,?)", (bid, "legacy", s["name"], now))
            conn.execute("INSERT INTO source_states(batch_id,source,output_mode,created_at) VALUES (?,?,?,?)",
                         (bid, s["name"], s["output_mode"], now))

def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript(SCHEMA)
        _migrate(conn)
        conn.executescript(TRIGGERS)
        if conn.execute("SELECT COUNT(*) c FROM sources").fetchone()["c"] == 0:
            now = _now()
            conn.executemany("INSERT INTO sources(name,sla_minutes,output_mode) VALUES (?,?,?)", [
                ("订单源", 15, "可写"), ("支付源", 20, "降级只读"), ("物流源", 30, "冻结"),
            ])
            conn.execute("INSERT INTO batches VALUES ('B-SEED-S1','seed','订单源',?)", (now,))
            conn.execute("INSERT INTO source_states(batch_id,source,output_mode,created_at) VALUES ('B-SEED-S1','订单源','可写',?)", (now,))
            conn.execute("INSERT INTO batches VALUES ('B-SEED-A1','arrival','支付源',?)", (now,))
            conn.execute("INSERT INTO arrivals(batch_id,source,source_clock,ingest_clock,lag_minutes,level) VALUES ('B-SEED-A1','支付源','2026-11-16T08:00:00','2026-11-16T08:35:00',35,'告警')")
            conn.execute("INSERT INTO source_states(batch_id,source,output_mode,created_at) VALUES ('B-SEED-A1','支付源','降级只读',?)", (now,))
            conn.execute("INSERT INTO batches VALUES ('B-SEED-A2','arrival','物流源',?)", (now,))
            conn.execute("INSERT INTO arrivals(batch_id,source,source_clock,ingest_clock,lag_minutes,level) VALUES ('B-SEED-A2','物流源','2026-11-16T07:00:00','2026-11-16T09:10:00',130,'违约')")
            conn.execute("INSERT INTO source_states(batch_id,source,output_mode,created_at) VALUES ('B-SEED-A2','物流源','冻结',?)", (now,))

def level_of(lag, sla):
    if lag <= sla: return "正常"
    if lag <= sla * 2: return "告警"
    return "违约"

def mode_of(level):
    return {"正常":"可写", "告警":"降级只读", "违约":"冻结"}[level]

# 半成锁:调度与入库同时写同一源须占锁,后交只记半成争用
_LOCKS: dict[str, threading.Lock] = {}
_LOCKS_GUARD = threading.Lock()

def _source_lock(name: str) -> threading.Lock:
    with _LOCKS_GUARD:
        return _LOCKS.setdefault(name, threading.Lock())

def _record_event(kind: str, source: str, detail: str):
    with connect() as c:
        c.execute("INSERT INTO half_success_events(kind,source,detail,created_at) VALUES (?,?,?,?)",
                  (kind, source, detail, _now()))

def _bound_write(source: str, path: str, write_fn):
    """乙路径:到达与源态同批事务号外键绑定,同败同成;先占半成锁,后交只记半成争用。"""
    lock = _source_lock(source)
    if not lock.acquire(blocking=False):
        _record_event("半成争用", source, f"{path}写路后交,被半成锁拦下,只留痕不落库")
        raise HTTPException(409, f"半成争用:源「{source}」另一写路在写,本次{path}只记争用")
    try:
        with connect() as c:
            return write_fn(c, "B" + datetime.now().strftime("%Y%m%d%H%M%S") + "-" + secrets.token_hex(4))
    except sqlite3.IntegrityError as e:
        _record_event("半成偏了", source, f"同批绑定被破坏,两侧拒收:{e}")
        raise HTTPException(409, "半成偏了:写入偏离半成切断说明,已拒绝并留痕")
    finally:
        lock.release()

@app.on_event("startup")
def _s(): init_db()

@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM sources").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "sources": n, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row: raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok]=row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/sources")
def sources(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM sources ORDER BY id").fetchall()]}

class ArrivalIn(BaseModel):
    source: str
    source_clock: str
    ingest_clock: str

@app.post("/api/arrivals")
def add_arrival(body: ArrivalIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    def w(c, bid):
        src = c.execute("SELECT * FROM sources WHERE name=?", (body.source,)).fetchone()
        if not src: raise HTTPException(404, "未知源")
        def parse(t):
            return datetime.fromisoformat(t)
        lag = int((parse(body.ingest_clock) - parse(body.source_clock)).total_seconds() // 60)
        if lag < 0: lag = 0
        lv = level_of(lag, src["sla_minutes"])
        mode = mode_of(lv)
        now = _now()
        c.execute("INSERT INTO batches(batch_id,kind,source,created_at) VALUES (?,?,?,?)", (bid, "arrival", body.source, now))
        c.execute("INSERT INTO arrivals(batch_id,source,source_clock,ingest_clock,lag_minutes,level) VALUES (?,?,?,?,?,?)",
                  (bid, body.source, body.source_clock, body.ingest_clock, lag, lv))
        c.execute("INSERT INTO source_states(batch_id,source,output_mode,created_at) VALUES (?,?,?,?)",
                  (bid, body.source, mode, now))
        c.execute("UPDATE sources SET output_mode=? WHERE name=?", (mode, body.source))
        return {"lag_minutes": lag, "level": lv, "output_mode": mode, "batch_id": bid}
    return _bound_write(body.source, "入库", w)

@app.get("/api/arrivals")
def arrivals(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM arrivals ORDER BY id DESC").fetchall()]}

@app.post("/api/catchup/{source}")
def catchup(source: str, authorization: str | None = Header(default=None)):
    require_user(authorization)
    def w(c, bid):
        src = c.execute("SELECT * FROM sources WHERE name=?", (source,)).fetchone()
        if not src: raise HTTPException(404, "未知源")
        now = _now()
        c.execute("INSERT INTO batches(batch_id,kind,source,created_at) VALUES (?,?,?,?)", (bid, "catchup", source, now))
        c.execute("INSERT INTO catchups(batch_id,source,status,restored_mode,note,created_at) VALUES (?,?,?,?,?,?)",
                  (bid, source, "成功", "可写", "补齐后恢复可写", now))
        c.execute("INSERT INTO source_states(batch_id,source,output_mode,created_at) VALUES (?,?,?,?)",
                  (bid, source, "可写", now))
        c.execute("UPDATE sources SET output_mode='可写' WHERE name=?", (source,))
        return {"ok": True, "output_mode": "可写", "batch_id": bid}
    return _bound_write(source, "调度", w)

@app.get("/api/catchups")
def catchups(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM catchups ORDER BY id DESC").fetchall()]}

@app.get("/api/half-success")
def half_success(authorization: str | None = Header(default=None)):
    require_user(authorization)
    return {"cut_path": CUT_PATH, "mechanism": CUT_MECHANISM, "active": CUT_MECHANISM,
            "inactive": CUT_INACTIVE, "sealed": True, "sealed_on": CUT_SEALED_ON,
            "doc": CUT_DOC, "lock": "半成锁(同源写路互斥,后交只记半成争用)"}

@app.get("/api/half-success/events")
def half_success_events(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM half_success_events ORDER BY id DESC").fetchall()]}

@app.get("/api/ledger")
def ledger(authorization: str | None = Header(default=None)):
    """对账账册:源态变迁与到达/补齐同批对账。"""
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""
          SELECT ss.id, ss.batch_id, b.kind, ss.source, ss.output_mode, ss.created_at,
                 a.id AS arrival_id, cp.id AS catchup_id
          FROM source_states ss
          JOIN batches b ON b.batch_id = ss.batch_id
          LEFT JOIN arrivals a ON a.batch_id = ss.batch_id
          LEFT JOIN catchups cp ON cp.batch_id = ss.batch_id
          ORDER BY ss.id DESC""").fetchall()
        return {"items":[dict(r) for r in rows]}

@app.get("/api/sources/{name}/detail")
def source_detail(name: str, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        src = c.execute("SELECT * FROM sources WHERE name=?", (name,)).fetchone()
        if not src: raise HTTPException(404, "未知源")
        arr = [dict(r) for r in c.execute("SELECT * FROM arrivals WHERE source=? ORDER BY id DESC LIMIT 10", (name,)).fetchall()]
        states = [dict(r) for r in c.execute("SELECT * FROM source_states WHERE source=? ORDER BY id DESC LIMIT 10", (name,)).fetchall()]
        events = [dict(r) for r in c.execute("SELECT * FROM half_success_events WHERE source=? ORDER BY id DESC LIMIT 10", (name,)).fetchall()]
        return {"source": dict(src), "arrivals": arr, "states": states, "events": events}

@app.get("/api/summary")
def summary(authorization: str | None = Header(default=None)):
    """水位卡片:与源目录、对账账册同出一批。"""
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT output_mode, COUNT(*) c FROM sources GROUP BY output_mode").fetchall()
        modes = {r["output_mode"]: r["c"] for r in rows}
        ev = {r["kind"]: r["c"] for r in c.execute("SELECT kind, COUNT(*) c FROM half_success_events GROUP BY kind").fetchall()}
        return {"sources": sum(modes.values()), "writable": modes.get("可写", 0),
                "degraded": modes.get("降级只读", 0), "frozen": modes.get("冻结", 0),
                "contentions": ev.get("半成争用", 0), "deviations": ev.get("半成偏了", 0)}

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")
app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
