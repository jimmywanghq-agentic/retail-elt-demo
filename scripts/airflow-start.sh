#!/usr/bin/env bash
# Run one local scheduler and webserver. This is a learning setup, not HA production.
set -euo pipefail
airflow db migrate
# Airflow needs a local UI login even though the software itself is open source.
airflow users list --output json | python -c 'import json,sys; sys.exit(0 if any(u["username"]=="demo" for u in json.load(sys.stdin)) else 1)' || 
  airflow users create --username demo --password demo_local_only --firstname Demo --lastname Learner --role Admin --email demo@example.invalid
airflow scheduler &
scheduler_pid=$!
trap 'kill "$scheduler_pid" 2>/dev/null || true' EXIT
airflow webserver --port 8080 &
web_pid=$!
trap 'kill "$scheduler_pid" "$web_pid" 2>/dev/null || true' EXIT
# Exit if either process dies, rather than showing a healthy UI with a dead scheduler.
wait -n "$scheduler_pid" "$web_pid"
