"""样例磁盘与备份链的种子数据。

样例盘：8 个定长块，每块 64 字节。恢复点布局（seq 即显示编号）：

  P0 全备      写入槽位 0..7，基础内容 A
  P1 增量      改 2、4（B）
  P2 增量      改 5（C），显式清零 1
  P3 增量      改 0（D），显式清零 3
  P4 增量      改 1（E，覆盖 P2 的清零），显式清零 6，改 7（E）

这样同时覆盖：
  * 缺省槽位沿链继承（如 0 号槽在 P1/P2 仍是 A，直到 P3 变 D）
  * 显式清零后再被写入“复活”（1 号槽 P2 清零、P4 重写）
  * 清零持续到末端（3 号槽 P3 清零、6 号槽 P4 清零）
"""
from __future__ import annotations

import hashlib
import sqlite3

from .chain import ZERO_HASH, set_meta

BLOCK_SIZE = 64
BLOCK_COUNT = 8


def block_content(letter: str, idx: int) -> bytes:
    """确定性的样例块内容，长度恰好 BLOCK_SIZE。"""
    base = f"{letter}|block-{idx}|".encode("ascii")
    return base + b"." * (BLOCK_SIZE - len(base))


# 每个恢复点：(seq, kind, 标签, [(槽位, 字母或 None=显式清零), ...])
PLAN = [
    (0, "full", "全备（样例盘基线）", [(i, "A") for i in range(BLOCK_COUNT)]),
    (1, "inc", "增量 P1：更新 2、4", [(2, "B"), (4, "B")]),
    (2, "inc", "增量 P2：更新 5，清零 1", [(5, "C"), (1, None)]),
    (3, "inc", "增量 P3：更新 0，清零 3", [(0, "D"), (3, None)]),
    (4, "inc", "增量 P4：更新 1、7，清零 6", [(1, "E"), (6, None), (7, "E")]),
]


def _hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _ensure_block(conn: sqlite3.Connection, content: bytes, is_zero: bool) -> str:
    h = ZERO_HASH if is_zero else _hash(content)
    conn.execute(
        "INSERT OR IGNORE INTO blocks(hash, size, is_zero, content) VALUES(?, ?, ?, ?)",
        (h, len(content), 1 if is_zero else 0, content),
    )
    return h


def _timestamp(seq: int) -> str:
    # 固定时间戳，保证样例可复现
    return f"2026-11-23T0{9 + seq}:00:00+00:00"


def seed_if_empty(conn: sqlite3.Connection) -> bool:
    """空库时写入样例链；已有数据则不动。返回是否执行了初始化。"""
    if conn.execute("SELECT 1 FROM backups LIMIT 1").fetchone():
        return False

    set_meta(conn, "block_size", str(BLOCK_SIZE))
    set_meta(conn, "block_count", str(BLOCK_COUNT))
    set_meta(conn, "chain_version", "0")
    _ensure_block(conn, b"", is_zero=True)

    for seq, kind, label, changes in PLAN:
        backup_id = f"seed-{seq:02d}"
        conn.execute(
            "INSERT INTO backups(id, seq, kind, label, created_at) VALUES(?, ?, ?, ?, ?)",
            (backup_id, seq, kind, label, _timestamp(seq)),
        )
        for idx, letter in changes:
            is_zero = letter is None
            content = b"" if is_zero else block_content(letter, idx)
            h = _ensure_block(conn, content, is_zero)
            conn.execute(
                "INSERT INTO backup_blocks(backup_id, block_index, block_hash, is_zero) "
                "VALUES(?, ?, ?, ?)",
                (backup_id, idx, h, 1 if is_zero else 0),
            )
    conn.commit()
    return True


def expected_image(seq: int) -> bytes:
    """独立按 PLAN 折叠出某恢复点应还原出的镜像，供自测做真值比对。"""
    state: dict[int, str | None] = {}
    for s, _kind, _label, changes in PLAN:
        if s > seq:
            break
        for idx, letter in changes:
            state[idx] = letter
    out = bytearray()
    for i in range(BLOCK_COUNT):
        letter = state[i]
        if letter is None:
            out += b"\x00" * BLOCK_SIZE
        else:
            out += block_content(letter, i)
    return bytes(out)
