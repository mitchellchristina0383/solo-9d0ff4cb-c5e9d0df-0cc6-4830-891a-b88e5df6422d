"""SQLite 连接与建表。

库内同时保存：
  * 定长块的唯一内容（blocks）
  * 恢复点（backups）与恢复点到块的指针（backup_blocks）
  * 槽位索引（slot_pointers）：每个槽位按恢复点 seq 记录“该点是否写过”，
    让还原时每条槽位能直达“目标点及以前最后一次写入”
  * 清退合并任务及其暂存区（merge_jobs / staged_blocks）

块内容只存一份，多个恢复点通过指针共享；清退中间恢复点时只改指针，
不复制块内容，也不会在盘上生成临时整盘镜像。

slot_pointers 与 backup_blocks 一一对应（每个落盘的指针一条索引），
不是“每个恢复点 × 每个槽位”的全量展开，因此索引大小只随链中实际保存的
指针数增长；被后继覆盖的旧指针仍保留，供还原更早的恢复点使用。
"""
from __future__ import annotations

import sqlite3

SLOT_INDEX_VERSION_KEY = "slot_index_version"
SLOT_INDEX_VERSION = "1"

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS backups (
    id          TEXT PRIMARY KEY,           -- UUID，终身不变
    seq         INTEGER NOT NULL UNIQUE,    -- 显示编号，清退后出现缺口，不重排
    kind        TEXT NOT NULL CHECK (kind IN ('full', 'inc')),
    label       TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS blocks (
    hash       TEXT PRIMARY KEY,            -- 内容哈希，去重用
    size       INTEGER NOT NULL,
    is_zero    INTEGER NOT NULL,            -- 1 表示“显式清零标记块”
    content    BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS backup_blocks (
    backup_id   TEXT NOT NULL REFERENCES backups(id),
    block_index INTEGER NOT NULL,
    block_hash  TEXT NOT NULL REFERENCES blocks(hash),
    is_zero     INTEGER NOT NULL,           -- 该恢复点在此槽位是否显式清零
    PRIMARY KEY (backup_id, block_index)
);

-- 还原索引：一个落盘指针一行，(槽位, seq) 为主键。
-- “目标点及以前最后一次写入”= 同一 block_index 下 seq <= 目标 seq 的最大 seq，
-- 走主键倒序 LIMIT 1 即可，无需折叠前面所有恢复点。
-- 索引行数 == backup_blocks 行数，随实际指针数增长。
CREATE TABLE IF NOT EXISTS slot_pointers (
    block_index INTEGER NOT NULL,
    seq         INTEGER NOT NULL,
    block_hash  TEXT NOT NULL REFERENCES blocks(hash),
    is_zero     INTEGER NOT NULL,
    PRIMARY KEY (block_index, seq)
) WITHOUT ROWID;

-- 清退发布按 seq 精确删除/重建两个参与点的索引行
CREATE INDEX IF NOT EXISTS idx_slot_pointers_seq
    ON slot_pointers (seq);

CREATE TABLE IF NOT EXISTS merge_jobs (
    id              TEXT PRIMARY KEY,       -- UUID
    evict_id        TEXT NOT NULL,          -- 待清退的中间恢复点
    based_on_version INTEGER NOT NULL,      -- 准备时依据的链版本（乐观并发）
    state           TEXT NOT NULL CHECK (state IN ('ready', 'published', 'stale')),
    created_at      TEXT NOT NULL,
    published_at    TEXT
);

-- 发布合并用的暂存区：只记录指针层改动，原链在发布前完全不动
CREATE TABLE IF NOT EXISTS staged_blocks (
    job_id      TEXT NOT NULL REFERENCES merge_jobs(id),
    block_index INTEGER NOT NULL,
    block_hash  TEXT NOT NULL REFERENCES blocks(hash),
    is_zero     INTEGER NOT NULL,
    PRIMARY KEY (job_id, block_index)
);

CREATE INDEX IF NOT EXISTS idx_bb_lookup
    ON backup_blocks (backup_id, block_index);
"""


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    # 外键打开；WAL 让“读原链”和“写暂存区”互不阻塞
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def insert_pointer(
    conn: sqlite3.Connection,
    backup_id: str,
    seq: int,
    block_index: int,
    block_hash: str,
    is_zero: int,
) -> None:
    """新增一个指针时同步写索引（两表同一事务，调用方负责 commit）。

    索引行数因此始终等于指针行数，新增恢复点不会把索引放大成
    “恢复点 × 槽位”的全量矩阵。
    """
    conn.execute(
        "INSERT INTO backup_blocks(backup_id, block_index, block_hash, is_zero) "
        "VALUES(?, ?, ?, ?)",
        (backup_id, block_index, block_hash, is_zero),
    )
    conn.execute(
        "INSERT INTO slot_pointers(block_index, seq, block_hash, is_zero) "
        "VALUES(?, ?, ?, ?)",
        (block_index, seq, block_hash, is_zero),
    )


def _rebuild_slot_index_locked(conn: sqlite3.Connection) -> int:
    """假定已在写事务内：从当前已发布的指针重建整个索引。"""
    conn.execute("DELETE FROM slot_pointers")
    conn.execute(
        "INSERT INTO slot_pointers(block_index, seq, block_hash, is_zero) "
        "SELECT bb.block_index, b.seq, bb.block_hash, bb.is_zero "
        "FROM backup_blocks bb JOIN backups b ON b.id = bb.backup_id"
    )
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (SLOT_INDEX_VERSION_KEY, SLOT_INDEX_VERSION),
    )
    row = conn.execute("SELECT COUNT(*) AS c FROM slot_pointers").fetchone()
    return int(row["c"])


def rebuild_slot_index(conn: sqlite3.Connection) -> int:
    """全量重建索引（种子数据、自测造深链、人工修复时使用）。

    调用方若已处于事务中（如 seed），由调用方负责提交。
    """
    own = not conn.in_transaction
    if own:
        conn.execute("BEGIN IMMEDIATE")
    try:
        n = _rebuild_slot_index_locked(conn)
        if own:
            conn.commit()
        return n
    except Exception:
        if own:
            conn.rollback()
        raise


def migrate_slot_index(conn: sqlite3.Connection) -> bool:
    """旧库升级：没有索引版本标记时，从 backup_blocks 原地回填。

    回填与写版本标记在同一事务内完成；旧库启动后即可直接下载原镜像，
    调用方无需先跑任何额外命令。返回是否执行了迁移。
    """
    own = not conn.in_transaction
    if own:
        conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT value FROM meta WHERE key = ?", (SLOT_INDEX_VERSION_KEY,)
        ).fetchone()
        if row is not None and row["value"] == SLOT_INDEX_VERSION:
            if own:
                conn.commit()
            return False
        _rebuild_slot_index_locked(conn)
        if own:
            conn.commit()
        return True
    except Exception:
        if own:
            conn.rollback()
        raise


def slot_index_matches(conn: sqlite3.Connection) -> bool:
    """索引与已发布指针是否逐行一致（自测 / 运维诊断用）。"""
    missing = conn.execute(
        "SELECT COUNT(*) AS c FROM ("
        "  SELECT bb.block_index, b.seq, bb.block_hash, bb.is_zero "
        "  FROM backup_blocks bb JOIN backups b ON b.id = bb.backup_id "
        "  EXCEPT "
        "  SELECT block_index, seq, block_hash, is_zero FROM slot_pointers"
        ")"
    ).fetchone()["c"]
    extra = conn.execute(
        "SELECT COUNT(*) AS c FROM ("
        "  SELECT block_index, seq, block_hash, is_zero FROM slot_pointers "
        "  EXCEPT "
        "  SELECT bb.block_index, b.seq, bb.block_hash, bb.is_zero "
        "  FROM backup_blocks bb JOIN backups b ON b.id = bb.backup_id"
        ")"
    ).fetchone()["c"]
    return missing == 0 and extra == 0


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()
    # 旧库（无索引表 / 无版本标记）在这里自动回填；新库只设置版本标记。
    migrate_slot_index(conn)
