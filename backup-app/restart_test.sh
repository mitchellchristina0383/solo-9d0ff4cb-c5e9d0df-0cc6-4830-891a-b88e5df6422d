#!/usr/bin/env bash
# 真实进程的中断/重启验证
set -u
export PYTHONPATH=/tmp/pylibs:/workspace/backup-app
export BACKUP_DB=/tmp/livedata/live.db
rm -rf /tmp/livedata && mkdir -p /tmp/livedata

start() {
  nohup python3 -m app.server > "$1" 2>&1 &
  echo $!
}
wait_http() {
  for _ in $(seq 1 20); do
    curl -s http://127.0.0.1:8000/healthz >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  return 1
}

echo "== 首次启动（全新库）=="
PID=$(start /tmp/livedata/s1.log)
wait_http
curl -s http://127.0.0.1:8000/healthz; echo

echo "== 下载 P4（真实流式 HTTP），校验镜像与索引一致性 =="
curl -s http://127.0.0.1:8000/backups/seed-04/download -o /tmp/livedata/p4-before.img
python3 - <<'PY'
import sqlite3, hashlib
from app.seed import expected_image
got = open('/tmp/livedata/p4-before.img','rb').read()
c = sqlite3.connect('/tmp/livedata/live.db'); c.row_factory = sqlite3.Row
p = c.execute("SELECT COUNT(*) c FROM backup_blocks").fetchone()['c']
i = c.execute("SELECT COUNT(*) c FROM slot_pointers").fetchone()['c']
print(' P4 镜像与真值一致:', got == expected_image(4), 'len', len(got))
print(' 索引行数==指针行数:', i == p, f'({i} vs {p})')
PY

echo "== 准备一笔清退但不发布（中断在准备期间）=="
JOB=$(curl -s -X POST http://127.0.0.1:8000/api/backups/seed-02/prepare \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['job_id'])")
echo "unpublished job: $JOB"
curl -s http://127.0.0.1:8000/api/jobs \
  | python3 -c "import sys,json;[print(' job',j['id'][:8],j['state']) for j in json.load(sys.stdin)]"

echo "== 准备期间下载应与准备前完全一致 =="
curl -s http://127.0.0.1:8000/backups/seed-04/download -o /tmp/livedata/p4-during-prep.img
cmp /tmp/livedata/p4-before.img /tmp/livedata/p4-during-prep.img \
  && echo " 准备期间下载一致: True"

echo "== SIGKILL 模拟中断 =="
kill -9 "$PID"
wait "$PID" 2>/dev/null
ls -la /tmp/livedata/live.db

echo "== 重启：应服务最后发布的链，半成品任务变 stale =="
PID=$(start /tmp/livedata/s2.log)
wait_http
curl -s http://127.0.0.1:8000/healthz; echo
curl -s http://127.0.0.1:8000/api/chain | python3 -c \
  "import sys,json;d=json.load(sys.stdin);print(' version:',d['chain_version'],'seqs:',[b['seq'] for b in d['backups']],'index==ptr:',d['slot_index_rows']==d['pointer_rows'])"
curl -s http://127.0.0.1:8000/api/jobs | python3 -c \
  "import sys,json;[print(' job',j['id'][:8],j['state']) for j in json.load(sys.stdin)]"

echo "== 重启后 P4 下载仍与真值一致（按最后成功发布的链）=="
curl -s http://127.0.0.1:8000/backups/seed-04/download -o /tmp/livedata/p4-after-restart.img
cmp /tmp/livedata/p4-before.img /tmp/livedata/p4-after-restart.img \
  && echo " 重启后下载一致: True"

echo "== 旧任务发布应被拒绝 =="
code=$(curl -s -o /tmp/livedata/err.json -w "%{http_code}" -X POST \
  "http://127.0.0.1:8000/api/jobs/$JOB/publish")
echo " http=$code body=$(cat /tmp/livedata/err.json)"

echo "== 重新发起清退并发布，应成功 =="
NEW=$(curl -s -X POST http://127.0.0.1:8000/api/backups/seed-02/prepare \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['job_id'])")
curl -s -X POST "http://127.0.0.1:8000/api/jobs/$NEW/publish"; echo
curl -s http://127.0.0.1:8000/backups/seed-04/download -o /tmp/livedata/p4-after.img
python3 -c "
from app.seed import expected_image
got=open('/tmp/livedata/p4-after.img','rb').read()
print(' P4 镜像与真值一致:', got==expected_image(4), 'len', len(got))
"
curl -s http://127.0.0.1:8000/api/chain | python3 -c \
  "import sys,json;d=json.load(sys.stdin);print(' 发布后 version:',d['chain_version'],'seqs:',[b['seq'] for b in d['backups']],'index==ptr:',d['slot_index_rows']==d['pointer_rows'])"
kill "$PID"
wait "$PID" 2>/dev/null
echo "== 完成 =="
