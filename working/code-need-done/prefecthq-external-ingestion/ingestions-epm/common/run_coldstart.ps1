<#
    Cold-start crawl 5 bang EPM - chay local tren Windows PowerShell.

    Khong dung Prefect server/worker: cold-start la viec mot lan, con deployment
    la cron lap lai. Co --standalone thi prefect_flow tu dung decorator gia nen
    khong can server, worker hay deployment.

    Cach dung:
        .\common\run_coldstart.ps1 -Check          # chi chay preflight
        .\common\run_coldstart.ps1 -Reset          # preflight + don sach (hoi xac nhan)
        .\common\run_coldstart.ps1 -Run            # chay crawl
        .\common\run_coldstart.ps1 -Reset -Run     # don sach roi crawl
        .\common\run_coldstart.ps1 -Run -Endpoint projects   # chi mot endpoint
#>
[CmdletBinding()]
param(
    [switch]$Check,
    [switch]$Reset,
    [switch]$Run,
    [string]$Endpoint = "",
    [string]$SparkMaster = "local[4]"
)

$ErrorActionPreference = "Stop"

# --------------------------------------------------------------- duong dan
$PROJ    = "E:\thinhnt22\crawler-prefecthq"
$VENV    = "E:\thinhnt22-code\epm\.venv"
$PYTHON  = Join-Path $VENV "Scripts\python.exe"

# --------------------------------------------------------------- moi truong
$env:JAVA_HOME = "C:\Program Files\Java\jdk1.8.0_202"
$env:Path      = "$env:JAVA_HOME\bin;$env:Path"

# Proxy cho API Clarizen. MinIO va Prefect nam tren localhost nen phai nam
# trong NO_PROXY, neu khong request se bi day qua proxy roi treo.
$env:HTTP_PROXY  = "http://192.168.5.8:3128"
$env:HTTPS_PROXY = "http://192.168.5.8:3128"
$env:NO_PROXY    = "127.0.0.1,localhost"
$env:no_proxy    = $env:NO_PROXY

# Venv nam ngoai thu muc du an -> phai chi ro cho Python tim package cua project
$env:PYTHONPATH        = $PROJ
$env:PYTHONUNBUFFERED  = "1"

# Noi Spark ghi parquet, va duong dan ghi vao LOCATION cua DDL Trino.
# Hai cai nay khac nhau vi file duoc tai tu MinIO roi upload tay len Ambari.
$env:WAREHOUSE_URI     = "s3a://lakehouse/warehouse"
$env:DDL_LOCATION_BASE = "/opt/datasets/crawlers/vcs/clarizen/data"

Set-Location $PROJ

if (-not (Test-Path $PYTHON)) { throw "Khong thay python venv: $PYTHON" }

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " Project        : $PROJ"
Write-Host " Python         : $PYTHON"
Write-Host " JAVA_HOME      : $env:JAVA_HOME"
Write-Host " WAREHOUSE_URI  : $env:WAREHOUSE_URI"
Write-Host " DDL_LOCATION   : $env:DDL_LOCATION_BASE"
Write-Host "==========================================================" -ForegroundColor Cyan

# ------------------------------------------------------------------ PREFLIGHT
if ($Check -or $Reset -or $Run) {
    Write-Host "`n>>> PREFLIGHT" -ForegroundColor Yellow
    & $PYTHON "common\check_env.py"
    if ($LASTEXITCODE -ne 0) { throw "Preflight that bai - dung lai." }
}
if ($Check -and -not ($Reset -or $Run)) { return }

# --------------------------------------------------------------------- RESET
if ($Reset) {
    Write-Host "`n>>> DON SACH" -ForegroundColor Yellow
    Write-Host "Se xoa:"
    Write-Host "  - data\warehouse\personal_raw\*  va  global_clean\*   (giu dlq)"
    Write-Host "  - data\staging\clarizen\*"
    Write-Host "  - checkpoints\*.checkpoint.json"
    Write-Host "  KHONG dong toi MinIO - xem ghi chu cuoi script."
    if ((Read-Host "Go 'YES' de xac nhan") -ne "YES") { throw "Da huy." }

    $stamp  = Get-Date -Format "yyyyMMdd_HHmmss"
    $backup = "_backup_pre_coldstart_$stamp.zip"
    $items  = @("data\warehouse", "checkpoints", "generated_ddl") |
              Where-Object { Test-Path $_ }
    if ($items) {
        Compress-Archive -Path $items -DestinationPath $backup -Force
        Write-Host "  sao luu -> $backup ($([math]::Round((Get-Item $backup).Length/1MB,2)) MB)"
    }

    # Chi xoa hai lop nay. dlq giu lai - ban ghi bi tu choi co schema rieng.
    foreach ($p in @("data\warehouse\personal_raw\*", "data\warehouse\global_clean\*",
                     "data\staging\clarizen\*", "checkpoints\*.checkpoint.json")) {
        if (Test-Path $p) { Remove-Item $p -Recurse -Force }
    }
    Write-Host "  da xoa." -ForegroundColor Green
}

# ----------------------------------------------------------------------- RUN
if ($Run) {
    Write-Host "`n>>> CRAWL (mode=full)" -ForegroundColor Yellow
    $started = Get-Date

    # Khong dat ten bien la $args - do la bien tu dong cua PowerShell
    $cliArgs = @("prefect_flow.py", "--standalone", "--mode", "full",
                 "--spark-master", $SparkMaster)
    if ($Endpoint) { $cliArgs += @("--endpoint", $Endpoint) } else { $cliArgs += "--all" }

    Write-Host "  $PYTHON $($cliArgs -join ' ')`n"
    & $PYTHON @cliArgs
    $code = $LASTEXITCODE

    $mins = [math]::Round(((Get-Date) - $started).TotalMinutes, 1)
    if ($code -ne 0) { throw "Crawl that bai (exit $code) sau $mins phut." }
    Write-Host "`n  Xong sau $mins phut." -ForegroundColor Green

    Write-Host "`n>>> DDL da sinh"
    Get-ChildItem "generated_ddl\*.sql" | ForEach-Object {
        "{0,-42} {1,8:N0} bytes" -f $_.Name, $_.Length
    }
}

Write-Host @"

----------------------------------------------------------
 BUOC TIEP THEO

 1) Tai parquet tu MinIO ve (console http://127.0.0.1:9001
    hoac mc), roi upload len Ambari vao dung thu muc:
       $env:DDL_LOCATION_BASE

 2) Chay file trong generated_ddl\ tren Trino, sau do:
       CALL hive.system.sync_partition_metadata(
            'personal_raw','projects','ADD');

 LUU Y: sink dung mode("append"). Chay lai script nay lan hai
 ma khong -Reset se NHAN DOI du lieu.

 Don MinIO: venv chua co boto3. Dung MinIO Console, hoac
   pip install boto3   roi chay clean_minio.py
----------------------------------------------------------
"@ -ForegroundColor DarkGray
