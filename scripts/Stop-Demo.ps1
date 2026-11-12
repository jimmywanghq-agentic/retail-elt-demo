# Stop containers without deleting your orders or Airflow history.
Set-Location (Split-Path $PSScriptRoot -Parent)
docker compose down
