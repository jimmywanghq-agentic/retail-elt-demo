# Run this after secrets/setup.sql succeeds. Preserve all other .env settings.
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$settings = Get-Content .env -Raw
$settings = $settings -replace '(?m)^SNOWFLAKE_USER=.*$', 'SNOWFLAKE_USER=ELT_SERVICE'
$settings = $settings -replace '(?m)^SNOWFLAKE_ROLE=.*$', 'SNOWFLAKE_ROLE=ELT_ROLE'
Set-Content -LiteralPath .env -Value $settings -Encoding utf8
Write-Host 'Local .env now uses the restricted ELT_SERVICE user.'
