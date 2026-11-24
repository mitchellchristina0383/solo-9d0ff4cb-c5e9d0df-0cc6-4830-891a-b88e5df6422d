"""SQLite 连接与建表。

库内同时保存：
  * 定长块的唯一内容（blocks）
  * 恢复点（backups）与恢复点到块的指针（backup_blocks）
  * 清退合并任务及其暂存区（merge_jobs / staged_blocks）

块内容只存一份，多个恢复点通过指针共享；清退中间恢复点时只改指针，
不复制块内容，也不会在盘上生成临时整盘镜像。
"""
from __future__ import annotations

import sqlite3

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


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()
