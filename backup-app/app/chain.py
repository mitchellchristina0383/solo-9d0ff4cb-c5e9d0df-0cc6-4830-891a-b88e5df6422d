"""备份链的领域逻辑：继承还原、流式出镜像、中间恢复点清退。

设计要点
--------
1. 每个槽位的“当前值”沿链向后继承：恢复点没有显式记录的槽位，取前一个
   恢复点的值（一直回溯到全备）。
2. 显式清零用 is_zero=1 的标记块表示，与“未写入（继承）”严格区分。
3. 清退 = 把待清退恢复点独有、其后继尚未覆盖的槽位指针，并入后继；
   全程只动指针和暂存表，块内容不复制，不生成临时整盘。
4. 合并分“准备 / 发布”两步。准备只写暂存区，原链可继续还原；发布用
   chain_version 做乐观并发（CAS），同一旧链上只有先发布的一笔生效。
5. 恢复点 UUID 与显示编号 seq 终身不变，清退后 seq 出现缺口但不重排。
"""
from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime, timezone
from uuid import uuid4

VERSION_KEY = "chain_version"


class DomainError(Exception):
    """业务错误基类。"""


class NotFoundError(DomainError):
    """资源不存在（404）。"""


class ConflictError(DomainError):
    """状态冲突 / 乐观锁失败（409）。"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------- meta ----
def get_meta(conn: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def chain_version(conn: sqlite3.Connection) -> int:
    return int(get_meta(conn, VERSION_KEY, "0"))


def disk_spec(conn: sqlite3.Connection) -> tuple[int, int]:
    """返回 (块大小, 块数量)。"""
    return (
        int(get_meta(conn, "block_size")),
        int(get_meta(conn, "block_count")),
    )


# ------------------------------------------------------------- backups ----
def list_backups(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(conn.execute("SELECT * FROM backups ORDER BY seq"))


def get_backup(conn: sqlite3.Connection, backup_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM backups WHERE id = ?", (backup_id,)).fetchone()
    if row is None:
        raise NotFoundError(f"恢复点不存在: {backup_id}")
    return row


def list_pointers(conn: sqlite3.Connection, backup_id: str) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            "SELECT block_index, block_hash, is_zero FROM backup_blocks "
            "WHERE backup_id = ? ORDER BY block_index",
            (backup_id,),
        )
    )


def chain_snapshot(conn: sqlite3.Connection) -> dict:
    block_size, block_count = disk_spec(conn)
    backups = []
    for b in list_backups(conn):
        rows = list_pointers(conn, b["id"])
        backups.append(
            {
                "id": b["id"],
                "seq": b["seq"],
                "kind": b["kind"],
                "label": b["label"],
                "created_at": b["created_at"],
                "explicit_blocks": len(rows),
                "zero_blocks": sum(1 for r in rows if r["is_zero"]),
            }
        )
    return {
        "chain_version": chain_version(conn),
        "block_size": block_size,
        "block_count": block_count,
        "pointer_rows": conn.execute(
            "SELECT COUNT(*) AS c FROM backup_blocks"
        ).fetchone()["c"],
        "slot_index_rows": conn.execute(
            "SELECT COUNT(*) AS c FROM slot_pointers"
        ).fetchone()["c"],
        "backups": backups,
    }


# -------------------------------------------------------------- restore ---
ZERO_HASH = hashlib.sha256(b"ZERO-MARKER-v1").hexdigest()


def _block_content(conn: sqlite3.Connection, block_hash: str) -> bytes:
    row = conn.execute(
        "SELECT content FROM blocks WHERE hash = ?", (block_hash,)
    ).fetchone()
    if row is None:  # pragma: no cover - 数据完整性兜底
        raise DomainError(f"块内容缺失: {block_hash}")
    return row["content"]


SLOT_POINTER_SQL = (
    "SELECT block_hash, is_zero FROM slot_pointers "
    "WHERE block_index = ? AND seq <= ? ORDER BY seq DESC LIMIT 1"
)


def materialize_blocks(conn: sqlite3.Connection, backup_id: str):
    """按块生成还原镜像的生成器（流式响应，不落临时整盘）。

    缺省块向前继承；显式清零块产出全零字节。每个槽位直接查索引
    “目标点及以前 seq 最大的那条指针”，不再逐恢复点折叠整条链，
    因此深链的取块开销与链中“实际保存的指针数”无关，只与该槽位的
    索引查找（主键倒序 LIMIT 1）有关。
    """
    target = get_backup(conn, backup_id)
    block_size, block_count = disk_spec(conn)
    target_seq = target["seq"]

    for i in range(block_count):
        # 一次索引点查：目标点及以前最后一次写入（跳过其后被覆盖的指针）
        row = conn.execute(SLOT_POINTER_SQL, (i, target_seq)).fetchone()
        if row is None:
            raise DomainError(f"槽位 {i} 无法沿链解析，数据不完整")
        if row["is_zero"]:
            yield b"\x00" * block_size
        else:
            yield _block_content(conn, row["block_hash"])


# -------------------------------------------------------------- eviction --
def list_jobs(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(conn.execute("SELECT * FROM merge_jobs ORDER BY created_at DESC"))


def get_job(conn: sqlite3.Connection, job_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM merge_jobs WHERE id = ?", (job_id,)).fetchone()
    if row is None:
        raise NotFoundError(f"合并任务不存在: {job_id}")
    return row


def prepare_eviction(conn: sqlite3.Connection, evict_id: str) -> dict:
    """第一步：在暂存区准备合并，原链一行都不改。"""
    ordered = list_backups(conn)
    by_id = {b["id"]: b for b in ordered}
    if evict_id not in by_id:
        raise NotFoundError(f"恢复点不存在: {evict_id}")

    position = next(i for i, b in enumerate(ordered) if b["id"] == evict_id)
    if position == 0:
        raise ConflictError("全备（链首）不能清退")
    if position == len(ordered) - 1:
        raise ConflictError("最后一个恢复点不能清退，没有后继可以承接块")

    if conn.execute(
        "SELECT 1 FROM merge_jobs WHERE state = 'ready' LIMIT 1"
    ).fetchone():
        raise ConflictError("已有一笔清退在准备/待发布中，请先发布或放弃它")

    succ = ordered[position + 1]
    target_rows = list_pointers(conn, evict_id)
    succ_indices = {r["block_index"] for r in list_pointers(conn, succ["id"])}

    # 后继尚未覆盖的槽位才需要并入；后继自己写过的（含再次清零）保持后继语义
    to_merge = [r for r in target_rows if r["block_index"] not in succ_indices]

    job_id = uuid4().hex
    base_version = chain_version(conn)
    conn.execute(
        "INSERT INTO merge_jobs(id, evict_id, based_on_version, state, created_at) "
        "VALUES(?, ?, ?, 'ready', ?)",
        (job_id, evict_id, base_version, _now()),
    )
    conn.executemany(
        "INSERT INTO staged_blocks(job_id, block_index, block_hash, is_zero) "
        "VALUES(?, ?, ?, ?)",
        [
            (job_id, r["block_index"], r["block_hash"], r["is_zero"])
            for r in to_merge
        ],
    )
    conn.commit()
    return {
        "job_id": job_id,
        "evict_id": evict_id,
        "evict_seq": by_id[evict_id]["seq"],
        "successor_id": succ["id"],
        "successor_seq": succ["seq"],
        "based_on_version": base_version,
        "merged_blocks": len(to_merge),
        "state": "ready",
    }


def publish_eviction(conn: sqlite3.Connection, job_id: str) -> dict:
    """第二步：CAS 原子发布。基线版本一变，本笔立即作废并提示重试。"""
    # IMMEDIATE 立刻拿写锁，把两笔并发发布在 SQLite 层串行化
    conn.execute("BEGIN IMMEDIATE")
    try:
        job = get_job(conn, job_id)
        if job["state"] == "published":
            raise ConflictError("该清退任务已发布，请勿重复提交")
        if job["state"] == "stale":
            raise ConflictError("该清退任务所依据的旧链已被其他清退改变，请重试")

        current_version = chain_version(conn)
        if current_version != job["based_on_version"]:
            conn.execute(
                "UPDATE merge_jobs SET state = 'stale' WHERE id = ?", (job_id,)
            )
            conn.commit()
            raise ConflictError(
                f"链版本已从 {job['based_on_version']} 变为 {current_version}，"
                "本笔清退依据的旧链已失效，请重试"
            )

        evict_id = job["evict_id"]
        ordered = list_backups(conn)
        position = next(
            (i for i, b in enumerate(ordered) if b["id"] == evict_id), None
        )
        if position is None or position == 0 or position == len(ordered) - 1:
            conn.execute(
                "UPDATE merge_jobs SET state = 'stale' WHERE id = ?", (job_id,)
            )
            conn.commit()
            raise ConflictError("恢复点位置已变化，无法按原计划清退，请重试")
        succ = ordered[position + 1]

        # 后继原有指针（在删除前读出来）与暂存区槽位合并
        old_succ = list_pointers(conn, succ["id"])
        staged = list(
            conn.execute(
                "SELECT block_index, block_hash, is_zero FROM staged_blocks "
                "WHERE job_id = ? ORDER BY block_index",
                (job_id,),
            )
        )
        staged_indices = {r["block_index"] for r in staged}
        merged = {r["block_index"]: (r["block_hash"], r["is_zero"]) for r in staged}
        for r in old_succ:
            if r["block_index"] not in staged_indices:
                merged[r["block_index"]] = (r["block_hash"], r["is_zero"])

        # 以下改动在同一事务内一次原子提交，外部要么看到旧链、要么看到新链：
        # 1) 后继指针整体替换为“原后继 + 并入槽位”
        conn.execute("DELETE FROM backup_blocks WHERE backup_id = ?", (succ["id"],))
        conn.executemany(
            "INSERT INTO backup_blocks(backup_id, block_index, block_hash, is_zero) "
            "VALUES(?, ?, ?, ?)",
            [
                (succ["id"], idx, h, z)
                for idx, (h, z) in sorted(merged.items())
            ],
        )
        # 2) 删除待清退恢复点的指针与恢复点本身（块内容因哈希共享而保留）
        conn.execute("DELETE FROM backup_blocks WHERE backup_id = ?", (evict_id,))
        conn.execute("DELETE FROM backups WHERE id = ?", (evict_id,))
        # 3) 索引只改参与合并的两个点：
        #    清退点的索引行全部移除；后继的索引行按新指针整体重建。
        #    其它恢复点的索引行一行不动，索引仍与“已发布指针”逐行对应。
        conn.execute("DELETE FROM slot_pointers WHERE seq = ?", (ordered[position]["seq"],))
        conn.execute("DELETE FROM slot_pointers WHERE seq = ?", (succ["seq"],))
        conn.executemany(
            "INSERT INTO slot_pointers(block_index, seq, block_hash, is_zero) "
            "VALUES(?, ?, ?, ?)",
            [(idx, succ["seq"], h, z) for idx, (h, z) in sorted(merged.items())],
        )
        # 4) 任务落账、链版本 +1
        conn.execute(
            "UPDATE merge_jobs SET state = 'published', published_at = ? WHERE id = ?",
            (_now(), job_id),
        )
        set_meta(conn, VERSION_KEY, str(current_version + 1))
        conn.commit()
        return {
            "job_id": job_id,
            "evicted_id": evict_id,
            "evicted_seq": ordered[position]["seq"],
            "successor_id": succ["id"],
            "successor_seq": succ["seq"],
            "new_version": current_version + 1,
            "merged_blocks": len(staged),
            "state": "published",
        }
    except ConflictError:
        # 上面已经 commit 的分支会走到这里；未 commit 的分支确保回滚
        if conn.in_transaction:
            conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise


def recover_pending_jobs(conn: sqlite3.Connection) -> int:
    """进程重启后调用：未发布的准备任务一律标记 stale。

    重启后系统只服务“最后发布的备份链”；半成品合并不自动续作，
    需要用户重新发起清退。
    """
    cur = conn.execute(
        "UPDATE merge_jobs SET state = 'stale' WHERE state = 'ready'"
    )
    conn.commit()
    return cur.rowcount


def abandon_job(conn: sqlite3.Connection, job_id: str) -> dict:
    """放弃一笔待发布的清退任务，清理暂存区，原链不受影响。"""
    job = get_job(conn, job_id)
    if job["state"] != "ready":
        raise ConflictError(f"任务状态为 {job['state']}，不能放弃")
    # 暂存区只删本任务的行；原链与索引在准备阶段从未改动，无需回滚
    conn.execute("DELETE FROM staged_blocks WHERE job_id = ?", (job_id,))
    conn.execute("DELETE FROM merge_jobs WHERE id = ?", (job_id,))
    conn.commit()
    return {"job_id": job_id, "state": "abandoned"}
