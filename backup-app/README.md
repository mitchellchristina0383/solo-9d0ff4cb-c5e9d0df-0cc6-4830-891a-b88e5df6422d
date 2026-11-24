# 备份链管理 · 中间恢复点清退（Flask + SQLite）

一块样例磁盘的**全备 + 按序增量**备份链网页：定长块、内容哈希去重存储；
可按恢复点下载还原镜像；可清退中间恢复点，把仍需保留的块**并入后继增量**，
全程只改指针、不生成临时整盘。

## 一、需求与实现对照

| 需求 | 实现 |
| --- | --- |
| 自带样例盘的全备 + 按序增量，定长块存储 | `app/seed.py`：8 块 × 64B，全备 P0 + 增量 P1~P4；块内容存 `blocks` 表，恢复点经 `backup_blocks` 指针引用，按 SHA-256 哈希去重 |
| 选恢复点下载还原镜像 | `GET /backups/<id>/download`，Flask 流式响应，逐块沿链折叠产出，`Content-Length` 固定为整盘大小，不落临时整盘 |
| 缺省块向前继承 | 还原时把 P0..目标点的指针按序折叠，未显式写入的槽位自然保留前一个恢复点的值 |
| 显式清零块还原为零 | 清零用 `is_zero=1` 的标记块表示，与“未写入（继承）”严格区分；还原产出 `\x00 * block_size`。清零后再被写入可“复活” |
| 清退中间点，把仍需保留的块并入后继，不生成临时整盘 | 两阶段：`prepare` 只把待并入槽位写入暂存表 `staged_blocks`；`publish` 在一个 SQLite 事务内把后继缺的槽位指针并入、删除被清退点。块内容因哈希共享保留，零整盘文件 |
| 其余恢复点编号和还原内容保持原样 | 恢复点 UUID 与显示编号 `seq` 终身不变，清退后 seq 出现缺口但不重排；自测对每个存活点逐字节比对清退前后镜像 |
| 合并准备期间仍可从原链还原 | 准备阶段原链一行不改，仅写暂存表；自测与真实 HTTP 均验证准备期间下载内容不变 |
| 重启后使用最后发布的备份链 | 已发布链即唯一持久链；启动时 `recover_pending_jobs` 把所有未发布的 `ready` 任务标记 `stale`，不自动续作 |
| 两次清退基于同一旧链，先发布生效、另一笔提示重试 | `merge_jobs.based_on_version` 记录基线 `chain_version`；发布用 `BEGIN IMMEDIATE` + 版本 CAS，版本不符返回 409「…旧链已失效，请重试」并标记 `stale`；基于新链重新准备即可成功 |
| Docker Compose 启动 | `Dockerfile`（python:3.11-slim + Flask 3.1.3）+ `docker-compose.yml`（命名卷 `backup-data` 持久化 `/data/backups.db`） |

### 数据模型（`app/db.py`）

- `meta`：链版本 `chain_version`、块大小、块数
- `backups`：恢复点（UUID、不变 seq、full/inc、标签、时间）
- `blocks`：块内容哈希去重，含一个全局零标记块
- `backup_blocks`：恢复点 → 槽位 → 块指针 + `is_zero`
- `merge_jobs` / `staged_blocks`：清退任务状态机（ready → published / stale）与暂存指针

### 关键并发与一致性

- SQLite 开 WAL、外键、`busy_timeout`；发布事务 `BEGIN IMMEDIATE` 先拿写锁，把并发发布串行化。
- 应用层另限制同时只允许一笔 `ready` 任务，准备即串行；真正的正确性由发布时的版本 CAS 兜底（即使绕过应用层直接插入同基线任务，后发布者仍 409）。
- 每个 HTTP 请求独立连接（`flask.g` + teardown 关闭）；流式下载用专用连接，迭代结束自行关闭。

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

- **T1 清零/继承**：P0~P4 五个镜像与独立折叠的真值逐字节一致；验证槽位继承、
  P2/P3 清零持续、P4 清零、清零后被 P4 重写“复活”。
- **T2 清退正确性**：清退 P2 并入 P3 后 5→4 点、seq 为 `[0,1,3,4]`（缺口不重排）、
  UUID 不变；四个存活点镜像与清退前逐字节一致；被清退点 404；重复发布 409；
  数据目录无任何 `.img/.tmp` 整盘文件。T2b 再清退 P1，版本 1→2，存活点仍逐字节一致。
- **T3 交错清退**：同基线（v2）两笔任务，先发布者成功（→v3），后发布者 409 且
  提示「请重试」、任务标 `stale`；基于新链重新准备发布成功。
- **T4 中断重启**：独立库先发布一笔（v1），再准备一笔未发布即重连模拟重启，
  `ready` 任务变 `stale`，服务的仍是最后发布的链（编号 `[0,1,3,4]`、各点镜像不变），
  旧任务发布 409，重新发起后成功（→v2）。
- **T5 HTTP 冒烟**：页面 200、下载 `Content-Length=512` 且哈希与真值一致、
  文件名带编号、不存在点 404、清退全备/末端点 409、工厂重启后无 ready 残留且版本保持。

### 2. 真实 HTTP 服务（独立进程 + curl/urllib）

- 下载 P4：512B，槽 3、槽 6 为零，槽 1=E、槽 0=D，符合预期。
- 准备期间再次下载，内容与准备前完全一致；第二笔准备被 409 拦截。
- 交错发布实测：API 任务与直接入库伪造的同基线任务，先发布成功 v2→v3，
  后发布返回 `409 链版本已从 2 变为 3，本笔清退依据的旧链已失效，请重试`。
- 中断重启实测（`restart_test.sh`，SIGKILL 后重启）：未发布任务重启后为 `stale`、
  链仍是最后发布版本、旧任务 409、重新清退成功、P4 镜像与真值一致。

## 四、余项与边界

- **未实际构建/运行镜像**：当前沙箱没有 Docker 守护进程，`docker compose up --build`
  仅做了 compose YAML 解析与 Python 编译校验；需在有 Docker 的环境执行确认。
- **开发服务器**：容器用 Flask 内置服务（单映射端口、演示规模）。生产多进程部署需换
  gunicorn/uwsgi，并保证发布串行化（现依赖单进程 + SQLite 写锁；多 worker 下版本 CAS
  仍正确，但建议加应用级互斥或单写者）。
- **块内容不物理回收**：清退只删指针，哈希共享的块内容保留在 `blocks` 表（题目要求不
  生成临时整盘、强调并入而非重写）。如后续要回收磁盘，可加“无引用块 GC”，需与发布事务同库处理。
- **单盘模型**：样例为一块磁盘的单条链；多盘/多链需在表上增加 disk 维度。
- **鉴权/审计**：演示未加认证与操作审计；合并任务表已保留全部历史（含 stale）可作审计基础。
