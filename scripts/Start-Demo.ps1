# Run this script from PowerShell. Paths are based on this script, not your current folder.
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
docker info --format '{{.ServerVersion}}'
if ($LASTEXITCODE -ne 0) { throw 'Open Docker Desktop and wait for the engine to start.' }
if (!(Test-Path .env)) { throw 'Create .env from .env.example first.' }
if (!(Test-Path secrets/snowflake_key.p8)) { throw 'Complete the Snowflake key setup described in README.md first.' }
docker compose build
if ($LASTEXITCODE -ne 0) { throw 'The Docker image could not be built.' }
# Fail early if the service key or Snowflake permissions are incorrect.
docker compose run --rm --no-deps orders-ui python scripts/test_snowflake.py
if ($LASTEXITCODE -ne 0) { throw 'Snowflake connection test failed. See the message above.' }
docker compose up -d
if ($LASTEXITCODE -ne 0) { throw 'The demo could not start.' }
Write-Host 'Order UI: http://localhost:8000'
Write-Host 'Airflow:  http://localhost:8080  (demo / demo_local_only)'
Write-Host 'Open powerbi/RetailDemo.pbip for the live Snowflake report.'
