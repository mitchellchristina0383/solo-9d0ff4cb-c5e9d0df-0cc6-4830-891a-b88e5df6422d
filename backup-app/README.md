# 备份链管理 · 中间恢复点清退（Flask + SQLite）

一块样例磁盘的**全备 + 按序增量**备份链网页：定长块、内容哈希去重存储；
可按恢复点下载还原镜像；可清退中间恢复点，把仍需保留的块**并入后继增量**，
全程只改指针、不生成临时整盘。

下载不再是“逐恢复点折叠整条链”，而是维护一份**槽位索引** `slot_pointers`：
每个槽位直接定位“所选恢复点及以前最后一次写入”，跳过其后已被覆盖的指针；
索引行数始终等于实际保存的指针行数，不随链长二次膨胀。

## 一、需求与实现对照

| 需求 | 实现 |
| --- | --- |
| 自带样例盘的全备 + 按序增量，定长块存储 | `app/seed.py`：8 块 × 64B，全备 P0 + 增量 P1~P4；块内容存 `blocks` 表，恢复点经 `backup_blocks` 指针引用，按 SHA-256 哈希去重 |
| 选恢复点下载还原镜像 | `GET /backups/<id>/download`，Flask 流式响应，逐块产出，`Content-Length` 固定为整盘大小，不落临时整盘 |
| 缺省块向前继承 | 还原时每槽位取“目标 seq 及以前 seq 最大的那条指针”，未显式写入的槽位自然取到前一个恢复点的值 |
| 显式清零块还原为零 | 清零用 `is_zero=1` 的标记块表示，与“未写入（继承）”严格区分；还原产出 `\x00 * block_size`。清零后再被写入可“复活” |
| 每个槽位直达所选恢复点及以前最后一次写入 | `slot_pointers(block_index, seq, ...)`，主键 `(block_index, seq)`；取块 SQL `WHERE block_index=? AND seq<=? ORDER BY seq DESC LIMIT 1` 走主键倒序点查 |
| 跳过已被覆盖的指针 | 上句 `seq<=目标` 只命中目标点及以前的最大 seq，目标点之后覆盖旧值的指针根本不会被读取 |
| 索引大小只随实际保存的指针数增长 | 每个落盘指针在 `slot_pointers` 中恰好一行；发布时只删/建两个参与点的行，`索引行数 == backup_blocks 行数`（自测断言之） |
| 旧 SQLite 库启动后即可下载原镜像 | `init_db` 调 `migrate_slot_index`：无 `slot_index_version` 标记时，在同一写事务里从 `backup_blocks` 回填索引并落标记；无需手工命令 |
| 清退中间点，把仍需保留的块并入后继，不生成临时整盘 | 两阶段：`prepare` 只把待并入槽位写入暂存表 `staged_blocks`；`publish` 在一个 SQLite 事务内把后继缺的槽位指针并入、删除被清退点 |
| 清退准备期间照常下载 | 准备阶段原链与索引一行不改，仅写暂存表；自测与真实 HTTP 均验证准备期间下载内容不变 |
| 发布时链和索引同时生效 | 指针表与索引表在同一 `BEGIN IMMEDIATE` 事务内一起提交，外部要么看到旧链+旧索引、要么看到新链+新索引 |
| 只更新参与合并两个点的索引 | 发布只 `DELETE FROM slot_pointers WHERE seq IN (被清退点, 后继)` 并按后继新指针重建其行；其余恢复点索引行完全不动（自测逐行比对） |
| 每次下载按同一份已发布的链流式还原 | 下载开始即 `BEGIN` 钉住 WAL 只读快照，整个流式过程读同一版本链；下载中途有清退发布也不影响本次内容 |
| 沿用继承和清零规则 | 索引只是把“折叠”换成“点查”，取到的块与清零语义完全一致；清退前后逐字节比对 |
| 发布失败/重启后仍按最后成功发布的链还原 | 发布 CAS 失败回滚（索引不变）；重启只跑 `recover_pending_jobs`，不重建索引，服务最后发布的链 |
| 其余恢复点编号和还原内容保持原样 | 恢复点 UUID 与显示编号 `seq` 终身不变，清退后 seq 出现缺口但不重排 |

### 数据模型（`app/db.py`）

- `meta`：链版本 `chain_version`、块大小、块数、索引版本 `slot_index_version`
- `backups`：恢复点（UUID、不变 seq、full/inc、标签、时间）
- `blocks`：块内容哈希去重，含一个全局零标记块
- `backup_blocks`：恢复点 → 槽位 → 块指针 + `is_zero`（权威数据）
- `slot_pointers`：还原索引 `(block_index, seq) → block_hash, is_zero`，与 `backup_blocks` 一一对应；`WITHOUT ROWID` 主键即覆盖查询，另有 `idx_slot_pointers_seq` 供发布按 seq 删除
- `merge_jobs` / `staged_blocks`：清退任务状态机（ready → published / stale）与暂存指针

### 索引如何保持正确

- 新增指针（种子）走 `insert_pointer()`，`backup_blocks` 与 `slot_pointers` 同事务写入。
- 清退发布在一个事务内：删除被清退点的索引行、删除后继旧索引行、按合并后的指针插入后继索引行；与 `backup_blocks` 的改动同时提交。
- 旧库升级用 `migrate_slot_index()` 全量回填一次，之后靠增量维护；`slot_index_matches()` 在自测中逐行校验两表一致。
- 索引行数 `==` 指针行数：没有“每个恢复点 × 每个槽位”的全量矩阵，链越深、指针越少，索引越小。

### 关键并发与一致性

- SQLite 开 WAL、外键、`busy_timeout`；发布事务 `BEGIN IMMEDIATE` 先拿写锁，把并发发布串行化。
- 应用层另限制同时只允许一笔 `ready` 任务，准备即串行；真正的正确性由发布时的版本 CAS 兜底（即使绕过应用层直接插入同基线任务，后发布者仍 409）。
- 每个 HTTP 请求独立连接（`flask.g` + teardown 关闭）；流式下载用专用连接，开始时 `BEGIN` 钉快照，迭代结束或响应关闭时释放。
- 发布失败（版本冲突）已 `commit` 的 `stale` 标记之外不触碰链与索引；未提交分支整体回滚。

## 二、如何运行

### Docker Compose（交付方式）

```bash
cd backup-app
docker compose up --build
# 浏览器打开 http://localhost:8000
```

数据保存在命名卷 `backup-data`（容器内 `/data/backups.db`），`docker compose down`
后再 `up` 仍使用最后发布的链。

### 本地直跑（无 Docker 时，本仓库自测即采用此方式）

```bash
python3 -m pip install -r requirements.txt
export BACKUP_DB=/tmp/backups.db
python3 -m app.server        # http://127.0.0.1:8000
```

### HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 管理页面 |
| GET | `/healthz` | 健康检查 + 当前链版本 |
| GET | `/api/chain` | 链与恢复点列表 |
| GET | `/api/jobs` | 清退任务列表 |
| POST | `/api/backups/<id>/prepare` | 准备清退（写暂存区，201） |
| POST | `/api/jobs/<job_id>/publish` | 原子发布（冲突 409） |
| POST | `/api/jobs/<job_id>/abandon` | 放弃待发布任务 |
| GET | `/backups/<id>/download` | 下载还原镜像 |

## 三、实测记录

环境：本机 Python 3.11.2 + Flask 3.1.3（沙箱无 Docker 守护进程，故 Dockerfile/Compose
做了严格 YAML 与编译校验，未实际 build；应用本身在同版本解释器+依赖下完整实测）。

### 1. 自动化自测 `python3 selftest.py` —— 64 项断言全部通过

原有链路与清退语义（T1~T5）全部保留通过，覆盖清零/继承、清退前后逐字节一致、
交错清退 CAS、重启服务最后发布链、HTTP 冒烟等。

### 2. 索引专项自测 `python3 selftest_index.py` —— 45 项断言全部通过

- **I1 深链取块开销**：在 5 点种子链后追加 400 个增量（共 405 点）。对同一目标点
  在追加前后用 `sqlite3.set_trace_callback` 统计取块 SQL 语句数，两者**完全相同**
  （19 条 = 取恢复点 1 + 块规格 2 + 8 槽位点查 + 8 次取内容）；即 O(块数)，与链长无关。
  `EXPLAIN QUERY PLAN` 显示走 `SEARCH ... USING PRIMARY KEY (block_index=? AND seq<?)`；
  索引行数 == 指针行数；中段镜像与独立回查一致。
- **I2 旧库升级**：用旧版表结构（无 `slot_pointers`）手工建库并写 P0~P4，直接
  `create_app(path)` 启动 → 自动回填索引并写 `slot_index_version`；P0~P4 五个点的
  HTTP 下载镜像全部与真值一致；二次启动不重复迁移、内容不变、索引仍逐行一致。
- **I3 清退前后**：准备期间索引行数与内容均不变、P4 下载不变；发布后
  `索引行数 == 指针行数`、四个存活点与清退前逐字节一致；用 trace 确认发布只对
  **两个参与点的 seq** 执行索引 DELETE，P4 的索引行发布前后完全相同；连续第二次清退后
  末端点仍正确。
- **I4 发布失败 / 重启**：基于旧链的迟到任务发布 409，链版本、索引行数、索引一致性
  均不变；重连并跑重启恢复后，链版本仍为最后成功发布的版本，未重建索引也逐行一致，
  P4 还原内容与重启前逐字节相同。
- **I5 下载与并发发布**：把流式下载停在第 1 块之后，用另一连接发布清退（版本 +1），
  再取剩余块；整次下载仍是**单一已发布链**的镜像（因下载连接以 `BEGIN` 钉住 WAL 快照）。

### 3. 真实 HTTP 服务（独立进程 + curl/urllib）

- 下载 P4：512B，哈希与真值一致；`索引行数==指针行数`。
- 准备期间再次下载，与准备前 `cmp` 完全一致；第二笔准备被 409 拦截。
- 中断重启实测（`restart_test.sh`，SIGKILL 后重启）：未发布任务重启后为 `stale`、
  链仍是最后发布版本、重启后 P4 下载不变、旧任务 409、重新清退成功、
  发布后 `seqs=[0,1,3,4]` 且 `索引行数==指针行数`、P4 镜像与真值一致。

## 四、余项与边界

- **未实际构建/运行镜像**：当前沙箱没有 Docker 守护进程，`docker compose up --build`
  仅做了 compose YAML 解析与 Python 编译校验；需在有 Docker 的环境执行确认。
- **索引是“按指针”而非“按槽位快照”**：每个保存过的指针一行，所以索引大小
  正比于链中实际指针数（题目要求），而不是恒定为“块数 × 恢复点”。若某恢复点显式
  指针极多，索引也随之线性增长；可在需要时对“无更早恢复点会再读到的指针”做压缩，
  但这会牺牲对早期点的还原能力，本期不做。
- **旧库升级是启动时一次性全量回填**：在 `BEGIN IMMEDIATE` 内完成，库很大时首次
  启动会短暂持写锁；回填量与 `backup_blocks` 行数成正比。演示规模可忽略。
- **开发服务器**：容器用 Flask 内置服务（单映射端口、演示规模）。生产多进程部署需换
  gunicorn/uwsgi，并保证发布串行化（现依赖单进程 + SQLite 写锁；多 worker 下版本 CAS
  仍正确，但建议加应用级互斥或单写者）。
- **块内容不物理回收**：清退只删指针与索引行，哈希共享的块内容保留在 `blocks` 表
  （题目要求不生成临时整盘、强调并入而非重写）。如后续要回收磁盘，可加“无引用块 GC”，
  需与发布事务同库处理。
- **单盘模型**：样例为一块磁盘的单条链；多盘/多链需在表上增加 disk 维度。
- **鉴权/审计**：演示未加认证与操作审计；合并任务表已保留全部历史（含 stale）可作审计基础。
