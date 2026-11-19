"""半成功切断:撕裂复现测 + 双路抢写测。

口径(见 docs/半成切断说明.md):
- 到达与源态同败同成:入库路写到达行与源态变迁必须同批落库,目录不得与到达列表撕裂。
- 调度与入库同时写同一源须占半成锁,后交只记半成争用并留痕。

两条测在缺陷未修前必须红:
1. test_half_success_tear_arrival_recorded_state_unchanged —— 复现"到达已挂源态未改"。
2. test_dual_writer_contention_traced —— 双路抢写,后交须被拒且留半成争用痕。
"""
import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient

import app as appmod
from app import app, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    init_db()
    yield


def _login(c: TestClient) -> dict:
    tok = c.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    return {"Authorization": "Bearer " + tok}


class _SpyConn(sqlite3.Connection):
    """让入库事务在写锁未放时挂起,等调度写敲门,强制两路交织。"""

    arrival_open: threading.Event
    knocking: threading.Event

    def execute(self, sql, *a, **kw):
        if isinstance(sql, str) and sql.startswith("INSERT INTO arrivals"):
            cur = super().execute(sql, *a, **kw)
            self.arrival_open.set()
            self.knocking.wait(10)
            return cur
        if isinstance(sql, str) and sql.startswith("UPDATE sources SET output_mode='可写'"):
            self.knocking.set()
        return super().execute(sql, *a, **kw)


def _race_arrival_vs_catchup(c: TestClient, h: dict, monkeypatch):
    """入库路(订单源滞后130分→违约→应冻结)与调度路(同源补齐→可写)同时写同一源。"""
    arrival_open, knocking = threading.Event(), threading.Event()
    _SpyConn.arrival_open, _SpyConn.knocking = arrival_open, knocking

    def patched_connect():
        conn = sqlite3.connect(appmod.DB_PATH, factory=_SpyConn)
        conn.row_factory = sqlite3.Row
        return conn

    monkeypatch.setattr(appmod, "connect", patched_connect)
    threading.Timer(2.0, knocking.set).start()  # 修复后调度写被半成锁拦下,不会敲门
    out = {}

    def do_arrival():
        r = c.post("/api/arrivals", headers=h, json={
            "source": "订单源", "source_clock": "2026-11-19T08:00:00",
            "ingest_clock": "2026-11-19T10:10:00"})
        out["arrival"] = r

    def do_catchup():
        out["catchup"] = c.post("/api/catchup/订单源", headers=h)

    t1 = threading.Thread(target=do_arrival)
    t1.start()
    arrival_open.wait(10)
    t2 = threading.Thread(target=do_catchup)
    t2.start()
    t1.join()
    t2.join()
    return out["arrival"], out["catchup"]


def test_half_success_tear_arrival_recorded_state_unchanged(tmp_path, monkeypatch):
    """半成失败测:到达已挂,源态不得停留在撕裂另一侧。"""
    c = TestClient(app)
    h = _login(c)
    r_arrival, _ = _race_arrival_vs_catchup(c, h, monkeypatch)
    assert r_arrival.status_code == 200

    cat = {s["name"]: s for s in c.get("/api/sources", headers=h).json()["items"]}["订单源"]
    latest = [a for a in c.get("/api/arrivals", headers=h).json()["items"]
              if a["source"] == "订单源"][0]
    expected = appmod.mode_of(latest["level"])
    assert cat["output_mode"] == expected, (
        f"撕裂:到达已挂(最新到达={latest['level']})源态未改"
        f"(目录={cat['output_mode']},应为{expected})")


def test_dual_writer_contention_traced(tmp_path, monkeypatch):
    """双路抢写失败测:后交只记半成争用——被拒且留痕,不得覆盖先交。"""
    c = TestClient(app)
    h = _login(c)
    r_arrival, r_catchup = _race_arrival_vs_catchup(c, h, monkeypatch)

    assert r_arrival.status_code == 200
    assert r_catchup.status_code == 409, "后交写路须被拒(半成争用),不得双双报成"
    assert "半成争用" in r_catchup.json()["detail"]

    events = c.get("/api/half-success/events", headers=h)
    assert events.status_code == 200
    kinds = [e["kind"] for e in events.json()["items"]]
    assert "半成争用" in kinds, "缺半成争用留痕"


def test_bound_batch_atomicity():
    """乙路径:到达与源态同批事务号外键绑定,同败同成。"""
    c = TestClient(app)
    h = _login(c)
    r = c.post("/api/arrivals", headers=h, json={
        "source": "订单源", "source_clock": "2026-11-19T08:00:00",
        "ingest_clock": "2026-11-19T09:00:00"})
    assert r.status_code == 200
    bid = r.json()["batch_id"]

    with appmod.connect() as conn:
        batch = conn.execute("SELECT * FROM batches WHERE batch_id=?", (bid,)).fetchone()
        arr = conn.execute("SELECT * FROM arrivals WHERE batch_id=?", (bid,)).fetchone()
        state = conn.execute("SELECT * FROM source_states WHERE batch_id=?", (bid,)).fetchone()
        cat = conn.execute("SELECT * FROM sources WHERE name='订单源'").fetchone()
    assert batch and arr and state, "到达与源态须同批落库"
    assert state["output_mode"] == cat["output_mode"] == r.json()["output_mode"]


def test_unknown_source_rejected_no_partial_write():
    """缺一方则两侧拒收:未知源不得留下半批残行。"""
    c = TestClient(app)
    h = _login(c)
    with appmod.connect() as conn:
        n_arr = conn.execute("SELECT COUNT(*) c FROM arrivals").fetchone()["c"]
        n_batch = conn.execute("SELECT COUNT(*) c FROM batches").fetchone()["c"]
    r = c.post("/api/arrivals", headers=h, json={
        "source": "幽灵源", "source_clock": "2026-11-19T08:00:00",
        "ingest_clock": "2026-11-19T09:00:00"})
    assert r.status_code == 404
    with appmod.connect() as conn:
        assert conn.execute("SELECT COUNT(*) c FROM arrivals").fetchone()["c"] == n_arr
        assert conn.execute("SELECT COUNT(*) c FROM batches").fetchone()["c"] == n_batch


def test_unbound_write_rejected_as_deviation():
    """偏离半成切断说明:未绑批号的写直接被库拒绝(半成偏了)。"""
    with appmod.connect() as conn:
        with pytest.raises(sqlite3.IntegrityError, match="半成偏了"):
            conn.execute("INSERT INTO arrivals(batch_id,source,source_clock,ingest_clock,lag_minutes,level)"
                         " VALUES ('B-GHOST','订单源','t','t',1,'正常')")


def test_five_views_aligned():
    """五处对齐:水位卡片、源目录、到达列表、详情、对账账册禁止撕裂。"""
    c = TestClient(app)
    h = _login(c)
    r = c.post("/api/arrivals", headers=h, json={
        "source": "订单源", "source_clock": "2026-11-19T08:00:00",
        "ingest_clock": "2026-11-19T10:10:00"})
    assert r.status_code == 200 and r.json()["output_mode"] == "冻结"
    bid = r.json()["batch_id"]

    cat = {s["name"]: s for s in c.get("/api/sources", headers=h).json()["items"]}["订单源"]
    latest_arr = [a for a in c.get("/api/arrivals", headers=h).json()["items"]
                  if a["source"] == "订单源"][0]
    detail = c.get("/api/sources/订单源/detail", headers=h).json()
    led = [x for x in c.get("/api/ledger", headers=h).json()["items"] if x["source"] == "订单源"][0]
    summ = c.get("/api/summary", headers=h).json()

    assert cat["output_mode"] == "冻结"                       # 源目录
    assert latest_arr["level"] == "违约" and latest_arr["batch_id"] == bid   # 到达列表
    assert detail["source"]["output_mode"] == "冻结"          # 详情
    assert detail["states"][0]["batch_id"] == bid and detail["states"][0]["output_mode"] == "冻结"
    assert led["batch_id"] == bid and led["output_mode"] == "冻结" and led["arrival_id"]  # 对账账册
    assert summ["frozen"] == 2 and summ["writable"] == 0      # 水位卡片(物流源亦冻结)


def test_cut_spec_sealed_and_labelled():
    """专册已封存;入库页标注口径:外键同批绑定生效、预查拒写未启用。"""
    from pathlib import Path
    doc = Path(__file__).resolve().parents[1] / "docs" / "半成切断说明.md"
    text = doc.read_text(encoding="utf-8")
    assert "乙路径" in text and "外键同批绑定" in text and "封存" in text and "2026-11-19" in text

    c = TestClient(app)
    h = _login(c)
    j = c.get("/api/half-success", headers=h).json()
    assert j["active"] == "外键同批绑定" and j["inactive"] == "预查拒写"
    assert j["sealed"] is True and j["sealed_on"] == "2026-11-19"
