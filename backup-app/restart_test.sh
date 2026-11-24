#!/usr/bin/env bash
# 真实进程的中断/重启验证
set -u
export PYTHONPATH=/workspace/tools/pylibs
export BACKUP_DB=/tmp/livedata/live.db
rm -f /tmp/livedata/live.db*

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

echo "== 准备一笔清退但不发布（中断在准备期间）=="
JOB=$(curl -s -X POST http://127.0.0.1:8000/api/backups/seed-02/prepare \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['job_id'])")
echo "unpublished job: $JOB"
curl -s http://127.0.0.1:8000/api/jobs \
  | python3 -c "import sys,json;[print(' job',j['id'][:8],j['state']) for j in json.load(sys.stdin)]"

echo "== SIGKILL 模拟中断 =="
kill -9 "$PID"
wait "$PID" 2>/dev/null
ls -la /tmp/livedata/live.db

echo "== 重启：应服务最后发布的链，半成品任务变 stale =="
PID=$(start /tmp/livedata/s2.log)
wait_http
curl -s http://127.0.0.1:8000/healthz; echo
curl -s http://127.0.0.1:8000/api/chain | python3 -c \
  "import sys,json;d=json.load(sys.stdin);print(' version:',d['chain_version'],'seqs:',[b['seq'] for b in d['backups']])"
curl -s http://127.0.0.1:8000/api/jobs | python3 -c \
  "import sys,json;[print(' job',j['id'][:8],j['state']) for j in json.load(sys.stdin)]"

echo "== 旧任务发布应被拒绝 =="
code=$(curl -s -o /tmp/livedata/err.json -w "%{http_code}" -X POST \
  "http://127.0.0.1:8000/api/jobs/$JOB/publish")
echo " http=$code body=$(cat /tmp/livedata/err.json)"

echo "== 重新发起清退并发布，应成功 =="
NEW=$(curl -s -X POST http://127.0.0.1:8000/api/backups/seed-02/prepare \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['job_id'])")
curl -s -X POST "http://127.0.0.1:8000/api/jobs/$NEW/publish"; echo
curl -s http://127.0.0.1:8000/backups/seed-04/download -o /tmp/livedata/p4-after.img
PYTHONPATH=/workspace/tools/pylibs:/workspace/backup-app python3 -c "
from app.seed import expected_image
got=open('/tmp/livedata/p4-after.img','rb').read()
print(' P4 镜像与真值一致:', got==expected_image(4), 'len', len(got))
"
kill "$PID"
wait "$PID" 2>/dev/null
echo "== 完成 =="
