"""针对“槽位索引 + 直取最后写入”的专项自测。

覆盖任务要求的四点：
  I1 深链取块开销：取一个恢复点不做 O(链长) 的全链折叠，SQL 工作量恒定
  I2 旧库升级：仅用 backup_blocks/backups 建出的旧库，启动后即可下载原镜像
  I3 清退前后还原一致，且发布只更新两个参与点的索引行
  I4 发布失败/重启后仍按最后成功发布的链还原；索引行数 == 指针行数

运行：PYTHONPATH=.:<flask路径> python3 selftest_index.py
"""
from __future__ import annotations

import hashlib
import os
import sqlite3
import tempfile

from app import chain
from app.db import (
    SLOT_INDEX_VERSION,
    SLOT_INDEX_VERSION_KEY,
    connect,
    init_db,
    slot_index_matches,
)
from app.seed import block_content, expected_image, seed_if_empty
from app.server import create_app

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} {detail}")


def image(conn, backup_id: str) -> bytes:
    return b"".join(chain.materialize_blocks(conn, backup_id))


# --------------------------------------------------------------- deep chain
def build_deep_chain(conn, n: int, block_count: int = 8) -> list[str]:
    """在种子链后追加 n 个增量，每个只改 1 个槽位，形成一条长链。

    返回全部恢复点 id（按 seq）。用于验证：深链下还原仍只做“槽位点查”。
    """
    from app.seed import _ensure_block, _timestamp
    from app.chain import set_meta

    set_meta(conn, "block_count", str(block_count))
    ids = [b["id"] for b in chain.list_backups(conn)]
    last_seq = max(b["seq"] for b in chain.list_backups(conn))
    for k in range(1, n + 1):
        seq = last_seq + k
        bid = f"deep-{seq:05d}"
        conn.execute(
            "INSERT INTO backups(id, seq, kind, label, created_at) VALUES(?, ?, 'inc', ?, ?)",
            (bid, seq, f"深链增量 {seq}", _timestamp(seq)),
        )
        idx = (seq * 7) % block_count
        letter = chr(ord("A") + (seq % 26))
        h = _ensure_block(conn, block_content(letter, idx), False)
        conn.execute(
            "INSERT INTO backup_blocks(backup_id, block_index, block_hash, is_zero) "
            "VALUES(?, ?, ?, 0)",
            (bid, idx, h),
        )
        ids.append(bid)
    conn.commit()
    from app.db import rebuild_slot_index

    rebuild_slot_index(conn)
    return ids


def sql_statements(conn, backup_id: str):
    counter = {"n": 0}

    def tracer(_stmt):
        counter["n"] += 1

    conn.set_trace_callback(tracer)
    try:
        data = b"".join(chain.materialize_blocks(conn, backup_id))
    finally:
        conn.set_trace_callback(None)
    return counter["n"], data


def test_deep_chain_fetch() -> None:
    print("I1 深链取块开销（索引点查，不做全链折叠）")
    tmp = tempfile.mkdtemp(prefix="backup-index-deep-")
    db = os.path.join(tmp, "deep.db")
    conn = connect(db)
    init_db(conn)
    seed_if_empty(conn)

    short_ids = [b["id"] for b in chain.chain_snapshot(conn)["backups"]]
    target_id = short_ids[-1]  # 种子链末端 P4，追加深链后其镜像不应改变
    short_n, short_data = sql_statements(conn, target_id)

    ids = build_deep_chain(conn, 400, block_count=8)
    check("深链长度达到 405 个恢复点", len(ids) == 405, str(len(ids)))
    rows = conn.execute("SELECT COUNT(*) c FROM slot_pointers").fetchone()["c"]
    expect = conn.execute("SELECT COUNT(*) c FROM backup_blocks").fetchone()["c"]
    check("索引行数 == 指针行数（只随实际指针增长）", rows == expect, f"{rows} vs {expect}")

    n_deep, data = sql_statements(conn, target_id)
    check("同一目标点在深链追加后内容不变", data == short_data)
    # 取块 SQL 数 = 1(取恢复点) + 2(块规格) + 每槽位 1 次点查 + 每非零块 1 次取内容
    # 与链长 405 完全无关：同一目标点在 5 点链与 405 点链上取块 SQL 数相同。
    check("深链取块 SQL 数与追加前完全相同（O(块数)，非 O(链长)）",
          n_deep == short_n, f"deep={n_deep} short={short_n}")
    check("深链取块 SQL 数有上限（≤ 3 + 8槽 + 8内容）",
          n_deep <= 3 + 8 + 8, f"{n_deep}")

    # 深链末端（全槽位都被深链写过）也能正确还原
    last_img = image(conn, ids[-1])
    check("深链末端还原长度正确", len(last_img) == 8 * 64)

    # 索引确实被用于“seq<=目标 的最后一条”，检查执行计划
    plan = " | ".join(
        r["detail"]
        for r in conn.execute(
            "EXPLAIN QUERY PLAN " + chain.SLOT_POINTER_SQL, (0, 4)
        )
    )
    check("索引点查走主键倒序（SEARCH ... block_index/seq）",
          "PRIMARY KEY" in plan and "SEARCH" in plan, plan)

    # 深链中间某点：早于它之后的覆盖都不能读到
    mid = ids[203]
    mid_img = image(conn, mid)
    later = ids[404]
    later_img = image(conn, later)
    check("深链中段与末端镜像不同（覆盖生效）", mid_img != later_img)
    first_byte_ok = True
    seqs = {b["id"]: b["seq"] for b in chain.list_backups(conn)}
    # 独立折叠真值：比较 mid 与“只取 seq<=mid”的指针结果
    want = bytearray()
    bs, bc = chain.disk_spec(conn)
    for i in range(bc):
        row = conn.execute(
            "SELECT block_hash,is_zero FROM slot_pointers WHERE block_index=? AND seq<=? "
            "ORDER BY seq DESC LIMIT 1",
            (i, seqs[mid]),
        ).fetchone()
        want += b"\x00" * bs if row["is_zero"] else conn.execute(
            "SELECT content FROM blocks WHERE hash=?", (row["block_hash"],)
        ).fetchone()["content"]
    check("深链中段镜像与独立回查一致", mid_img == bytes(want))
    check("槽位索引与已发布指针逐行一致", slot_index_matches(conn))
    conn.close()


# ------------------------------------------------------------- old database
def make_legacy_db(path: str) -> list[str]:
    """只用旧版表结构（无 slot_pointers）建库并种数据。"""
    from app.seed import BLOCK_COUNT, PLAN, block_content
    from app.chain import ZERO_HASH

    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE backups (
            id TEXT PRIMARY KEY, seq INTEGER NOT NULL UNIQUE,
            kind TEXT NOT NULL CHECK (kind IN ('full','inc')),
            label TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE blocks (
            hash TEXT PRIMARY KEY, size INTEGER NOT NULL,
            is_zero INTEGER NOT NULL, content BLOB NOT NULL);
        CREATE TABLE backup_blocks (
            backup_id TEXT NOT NULL REFERENCES backups(id),
            block_index INTEGER NOT NULL,
            block_hash TEXT NOT NULL REFERENCES blocks(hash),
            is_zero INTEGER NOT NULL,
            PRIMARY KEY (backup_id, block_index));
        CREATE INDEX idx_bb_lookup ON backup_blocks (backup_id, block_index);
        """
    )
    conn.execute("INSERT INTO meta VALUES('block_size','64')")
    conn.execute("INSERT INTO meta VALUES('block_count','8')")
    conn.execute("INSERT INTO meta VALUES('chain_version','0')")
    conn.execute(
        "INSERT INTO blocks(hash,size,is_zero,content) VALUES(?,?,1,?)",
        (ZERO_HASH, 0, b""),
    )
    for seq, kind, label, changes in PLAN:
        bid = f"seed-{seq:02d}"
        conn.execute(
            "INSERT INTO backups VALUES(?,?,?,?,?)",
            (bid, seq, kind, label, f"2026-11-23T0{9+seq}:00:00+00:00"),
        )
        for idx, letter in changes:
            is_zero = letter is None
            content = b"" if is_zero else block_content(letter, idx)
            h = ZERO_HASH if is_zero else hashlib.sha256(content).hexdigest()
            conn.execute(
                "INSERT OR IGNORE INTO blocks VALUES(?,?,?,?)",
                (h, len(content), 1 if is_zero else 0, content),
            )
            conn.execute(
                "INSERT INTO backup_blocks VALUES(?,?,?,?)",
                (bid, idx, h, 1 if is_zero else 0),
            )
    conn.commit()
    conn.close()
    return [f"seed-{s:02d}" for s in range(BLOCK_COUNT - 3)]


def test_legacy_db() -> None:
    print("\nI2 旧 SQLite 库启动后直接下载原镜像")
    tmp = tempfile.mkdtemp(prefix="backup-index-legacy-")
    db = os.path.join(tmp, "legacy.db")
    make_legacy_db(db)

    # 启动前：旧库没有 slot_pointers 表
    raw = sqlite3.connect(db)
    has_idx = raw.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='slot_pointers'"
    ).fetchone()
    raw.close()
    check("前置：旧库确实没有 slot_pointers 表", has_idx is None)

    # 只需按正常方式启动应用（create_app → init_db → migrate_slot_index）
    app = create_app(db)
    client = app.test_client()
    r = client.get("/healthz")
    check("旧库启动后健康检查正常", r.status_code == 200)

    snap = client.get("/api/chain").get_json()
    check("旧库 5 个恢复点均可列出", len(snap["backups"]) == 5)
    check("旧库迁移后索引行数 == 指针行数",
          snap["slot_index_rows"] == snap["pointer_rows"], str(snap))

    # 每个恢复点都能通过 HTTP 下载到与真值一致的原镜像
    seq_to_id = {b["seq"]: b["id"] for b in snap["backups"]}
    ok = True
    for seq in (0, 1, 2, 3, 4):
        resp = client.get(f"/backups/{seq_to_id[seq]}/download")
        if resp.status_code != 200 or resp.data != expected_image(seq):
            ok = False
    check("旧库启动后 P0~P4 下载镜像全部与真值一致", ok)

    conn = connect(db)
    check("迁移写入索引版本标记",
          conn.execute("SELECT value FROM meta WHERE key=?",
                       (SLOT_INDEX_VERSION_KEY,)).fetchone()["value"]
          == SLOT_INDEX_VERSION)
    check("迁移后索引与指针逐行一致", slot_index_matches(conn))

    # 二次启动不应重复迁移/破坏
    conn.close()
    app2 = create_app(db)
    c2 = app2.test_client()
    check("二次启动仍可下载且内容不变",
          c2.get(f"/backups/{seq_to_id[4]}/download").data == expected_image(4))
    conn = connect(db)
    check("二次启动后索引仍一致", slot_index_matches(conn))
    conn.close()


# ------------------------------------------------------- eviction roundtrip
def test_eviction_index() -> None:
    print("\nI3 清退前后还原一致，发布只改两个参与点的索引行")
    tmp = tempfile.mkdtemp(prefix="backup-index-evict-")
    db = os.path.join(tmp, "evict.db")
    conn = connect(db)
    init_db(conn)
    seed_if_empty(conn)
    snap = chain.chain_snapshot(conn)
    before = {b["id"]: image(conn, b["id"]) for b in snap["backups"]}
    seq_by_id = {b["id"]: b["seq"] for b in snap["backups"]}

    # 记录其它（非参与合并且非清退）恢复点的索引行，发布后应一行不变
    p2 = next(b for b in snap["backups"] if b["seq"] == 2)
    untouched_seq = 4
    untouched_before = conn.execute(
        "SELECT block_index, seq, block_hash, is_zero FROM slot_pointers "
        "WHERE seq=? ORDER BY block_index", (untouched_seq,)
    ).fetchall()

    prep = chain.prepare_eviction(conn, p2["id"])
    # 准备期间索引不动，下载照常
    check("准备期间索引与指针仍一致", slot_index_matches(conn))
    check("准备期间 P4 下载内容不变", image(conn, next(
        b["id"] for b in snap["backups"] if b["seq"] == 4)) == before[next(
        b["id"] for b in snap["backups"] if b["seq"] == 4)])
    check("准备期间索引行数未变",
          conn.execute("SELECT COUNT(*) c FROM slot_pointers").fetchone()["c"]
          == snap["slot_index_rows"])

    pub = chain.publish_eviction(conn, prep["job_id"])
    check("发布成功版本 +1", pub["new_version"] == 1)
    check("发布后索引与指针逐行一致", slot_index_matches(conn))

    after_snap = chain.chain_snapshot(conn)
    check("索引行数 == 指针行数（发布后）",
          after_snap["slot_index_rows"] == after_snap["pointer_rows"], str(after_snap))
    for b in after_snap["backups"]:
        check(f"清退后 P{b['seq']} 与清退前逐字节一致",
              image(conn, b["id"]) == before[b["id"]])

    untouched_after = conn.execute(
        "SELECT block_index, seq, block_hash, is_zero FROM slot_pointers "
        "WHERE seq=? ORDER BY block_index", (untouched_seq,)
    ).fetchall()
    check("发布未触碰第二个点以外的索引行（P4 行完全相同）",
          [tuple(r) for r in untouched_before] == [tuple(r) for r in untouched_after])

    # 用 SQL trace 检查发布语句只 DELETE 两个 seq 的索引行
    stmts: list[str] = []
    conn2 = conn

    def tracer(stmt):
        stmts.append(stmt)

    conn2.set_trace_callback(tracer)
    p3 = next(b["id"] for b in after_snap["backups"] if b["seq"] == 3)
    job = chain.prepare_eviction(conn2, p3)
    stmts.clear()
    chain.publish_eviction(conn2, job["job_id"])
    conn2.set_trace_callback(None)
    slot_deletes = [s for s in stmts if s.strip().upper().startswith("DELETE FROM SLOT_POINTERS")]
    check("发布的索引删除只针对参与合并的两个 seq",
          len(slot_deletes) == 2, str(slot_deletes))
    check("发布后索引仍与指针逐行一致（第二次清退）", slot_index_matches(conn2))

    # 清退后再取末端点，回落结果仍正确
    s3 = chain.chain_snapshot(conn2)
    last = s3["backups"][-1]
    check("连续清退后末端点还原仍正确",
          image(conn2, last["id"]) == before[last["id"]])
    conn2.close()


# ------------------------------------------- failed publish / restart
def test_publish_failure_and_restart() -> None:
    print("\nI4 发布失败 / 重启后按最后成功发布的链还原")
    tmp = tempfile.mkdtemp(prefix="backup-index-restart-")
    db = os.path.join(tmp, "restart.db")
    conn = connect(db)
    init_db(conn)
    seed_if_empty(conn)
    snap = chain.chain_snapshot(conn)
    before = {b["id"]: image(conn, b["id"]) for b in snap["backups"]}

    # 成功发布一笔：清退 P2
    p2 = next(b for b in snap["backups"] if b["seq"] == 2)
    chain.publish_eviction(conn, chain.prepare_eviction(conn, p2["id"])["job_id"])
    s1 = chain.chain_snapshot(conn)
    check("第一次发布后版本 1，编号 [0,1,3,4]",
          s1["chain_version"] == 1 and [b["seq"] for b in s1["backups"]] == [0, 1, 3, 4])

    s1_before = {b["id"]: image(conn, b["id"]) for b in s1["backups"]}

    # 制造一笔“基于旧链”的迟到任务，发布失败（409），索引必须保持发布前状态
    p3 = next(b for b in s1["backups"] if b["seq"] == 3)["id"]
    job_ok = chain.prepare_eviction(conn, p3)  # 基线 v1
    chain.abandon_job(conn, job_ok["job_id"])
    job_ok = chain.prepare_eviction(conn, p3)  # 再来一笔基线 v1
    chain.publish_eviction(conn, job_ok["job_id"])  # 先发布 → v2
    # 手工造一个基于 v2 之前（v1）的 ready 任务（清退 P3）
    from uuid import uuid4
    from app.chain import _now
    late_id = uuid4().hex
    conn.execute(
        "INSERT INTO merge_jobs(id, evict_id, based_on_version, state, created_at) "
        "VALUES(?, ?, 1, 'ready', ?)", (late_id, p3, _now()),
    )
    conn.commit()
    pre_fail_snapshot = chain.chain_snapshot(conn)
    conflict = False
    try:
        chain.publish_eviction(conn, late_id)
    except chain.ConflictError:
        conflict = True
    check("基于旧链的发布返回冲突", conflict)
    check("发布失败后链版本未变", chain.chain_snapshot(conn)["chain_version"] == 2)
    check("发布失败后索引与指针仍逐行一致", slot_index_matches(conn))
    check("发布失败后索引行数未变",
          chain.chain_snapshot(conn)["slot_index_rows"] == pre_fail_snapshot["slot_index_rows"])

    # 重启：新连接 + 启动恢复，索引不重建、原链照常
    conn.close()
    conn2 = connect(db)
    init_db(conn2)
    chain.recover_pending_jobs(conn2)
    restarted = chain.chain_snapshot(conn2)
    check("重启后链版本仍为最后成功发布的 2", restarted["chain_version"] == 2)
    check("重启后索引与指针逐行一致（未重建也正确）", slot_index_matches(conn2))
    # 重启装载后直接还原内容与真值一致（与重启前保存的镜像逐字节比对）
    expected_p4 = None
    for b in restarted["backups"]:
        if b["seq"] == 4:
            expected_p4 = s1_before.get(b["id"])
    check("重启后 P4 仍存在", expected_p4 is not None)
    check("重启装载后直接还原内容与重启前一致",
          image(conn2, next(b["id"] for b in restarted["backups"] if b["seq"] == 4))
          == expected_p4)
    conn2.close()


# --------------------------------------- download races a concurrent publish
def test_download_snapshot_race() -> None:
    print("\nI5 下载与并发发布：每次下载按同一份已发布链流式还原")
    tmp = tempfile.mkdtemp(prefix="backup-index-race-")
    db = os.path.join(tmp, "race.db")
    app = create_app(db)
    client = app.test_client()
    snap = client.get("/api/chain").get_json()
    p4 = next(b for b in snap["backups"] if b["seq"] == 4)
    want = expected_image(4)

    # 用 stream 迭代器把下载“停在半途”，在两次取块之间插入一次发布清退
    resp = client.get(f"/backups/{p4['id']}/download", buffered=False)
    it = resp.response
    first = next(it)
    # 服务端读连接已开 BEGIN 快照；这里用另一连接发布清退
    evict = next(b for b in snap["backups"] if b["seq"] == 2)
    conn = connect(db)
    job = chain.prepare_eviction(conn, evict["id"])
    pub = chain.publish_eviction(conn, job["job_id"])
    rest = b"".join(it)
    resp.close()
    got = first + rest
    check("下载过程中发布成功（链版本 +1）", pub["new_version"] == 1)
    check("跨越发布的同一次下载仍是单一已发布链的镜像", got == want,
          f"len={len(got)} sha={hashlib.sha256(got).hexdigest()[:12]}")
    check("下载完成后索引与发布后的指针一致", slot_index_matches(conn))
    conn.close()


def main() -> int:
    test_deep_chain_fetch()
    test_legacy_db()
    test_eviction_index()
    test_publish_failure_and_restart()
    test_download_snapshot_race()
    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
