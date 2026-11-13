#!/bin/sh
set -eu
if [ ! -f .env ]; then
  echo "Copy .env.example to .env and replace the credential placeholders before running the demo" >&2
  exit 1
fi
set -a
. ./.env
set +a
compose='docker compose'
$compose up --build -d
seed_output=$(go run ./cmd/job-recovery demo seed --count 100)
printf '%s\n' "$seed_output"
run_id=$(printf '%s\n' "$seed_output" | sed -n 's/^Seeded 100 orders in run //p')
case "$run_id" in
  ????????-????-????-????-????????????) ;;
  *) echo "could not read demo run UUID" >&2; exit 1 ;;
esac
# Wait until PostgreSQL proves worker-a holds a live execution lease.
active=0
for _ in $(seq 1 120); do
  active=$($compose exec -T postgres psql -U job -d job_recovery -v run_id="$run_id" -Atc "SELECT count(*) FROM jobs WHERE demo_run_id=:'run_id'::uuid AND status='RUNNING' AND lease_owner='worker-a' AND lease_expires_at>now()")
  [ "$active" -gt 0 ] && break
  sleep 0.25
done
if [ "$active" -lt 1 ]; then echo "worker-a never acquired a live lease for run $run_id" >&2; exit 1; fi
# Compose's default kill signal is SIGKILL.
$compose kill worker-a
for _ in $(seq 1 90); do
  counts=$($compose exec -T postgres psql -U job -d job_recovery -v run_id="$run_id" -Atc "SELECT d.expected_jobs,(SELECT count(*) FROM jobs j WHERE j.demo_run_id=d.id),(SELECT count(*) FROM jobs j WHERE j.demo_run_id=d.id AND j.status='SUCCEEDED'),(SELECT count(*) FROM jobs j JOIN order_fulfillments f ON f.job_id=j.id WHERE j.demo_run_id=d.id),(SELECT count(*) FROM (SELECT f.order_id FROM jobs j JOIN order_fulfillments f ON f.job_id=j.id WHERE j.demo_run_id=d.id GROUP BY f.order_id HAVING count(*)>1) q),(SELECT count(*) FROM job_events e JOIN jobs j ON j.id=e.job_id WHERE j.demo_run_id=d.id AND e.event_type='JOB_RECOVERED') FROM demo_runs d WHERE d.id=:'run_id'::uuid")
  expected=$(printf '%s' "$counts" | cut -d '|' -f1)
  total=$(printf '%s' "$counts" | cut -d '|' -f2)
  succeeded=$(printf '%s' "$counts" | cut -d '|' -f3)
  effects=$(printf '%s' "$counts" | cut -d '|' -f4)
  duplicates=$(printf '%s' "$counts" | cut -d '|' -f5)
  recovered=$(printf '%s' "$counts" | cut -d '|' -f6)
  [ "$expected" = 100 ] && [ "$total" = 100 ] && [ "$succeeded" = 100 ] && [ "$effects" = 100 ] && [ "$duplicates" = 0 ] && [ "$recovered" -gt 0 ] && {
    printf 'verified run=%s jobs=%s succeeded=%s fulfillments=%s duplicates=%s recovered=%s\n' "$run_id" "$total" "$succeeded" "$effects" "$duplicates" "$recovered"
    exit 0
  }
  sleep 1
done
echo "verification timed out for run $run_id; inspect jobs and recovery events" >&2
exit 1
