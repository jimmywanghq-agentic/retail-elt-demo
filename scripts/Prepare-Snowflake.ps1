param([Parameter(Mandatory=$true)][string]$ReportUsername)
# Prepare the exact SQL for review; this script does not change Snowflake.
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$opensslCommand = Get-Command openssl -ErrorAction SilentlyContinue
$opensslPath = if ($opensslCommand) { $opensslCommand.Source } else { 'C:\Program Files\Git\usr\bin\openssl.exe' }
if (!(Test-Path -LiteralPath $opensslPath)) { throw 'OpenSSL is required (Git for Windows includes it).' }
New-Item -ItemType Directory -Force -Path secrets | Out-Null
if (!(Test-Path secrets/snowflake_key.p8)) {
    # PKCS8 is understood by both the Snowflake Python connector and dbt.
    & $opensslPath genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out secrets/snowflake_key.p8 2>$null
    if ($LASTEXITCODE -ne 0) { throw 'Key generation failed.' }
}
& $opensslPath pkey -in secrets/snowflake_key.p8 -pubout -out secrets/snowflake_key.pub
if ($LASTEXITCODE -ne 0) { throw 'Public-key extraction failed.' }
# Limit host access to this demo key. Docker Desktop can still mount the file.
$identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
icacls secrets/snowflake_key.p8 /inheritance:r /grant:r "${identity}:(R,W)" | Out-Null
$publicKey = (Get-Content secrets/snowflake_key.pub | Where-Object { $_ -notmatch '^-----' }) -join ''
$template = Get-Content sql/snowflake_setup.sql -Raw
$template.Replace('__PUBLIC_KEY__', $publicKey).Replace('__REPORT_USER__', $ReportUsername.Replace('"','""')) | Set-Content secrets/setup.sql -Encoding utf8
Write-Host 'Review secrets/setup.sql, then run it in Snowsight as ACCOUNTADMIN.'
Write-Host 'The generated private key and setup SQL are excluded from Git.'
