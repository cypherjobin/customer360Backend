"""
Customer 360 - Production Installation Script (6:00 PM Schedule)
================================================================
Installs Customer 360 on a production Windows Server.
Daily pipeline runs at 6:00 PM (after ETL completes).

Usage:
    .\install_production.ps1

Run as Administrator!
"""

#Requires -RunAsAdministrator

$ErrorActionPreference = "Stop"
$InstallDir = "E:\Customer360"

Write-Host ""
Write-Host "=" * 70
Write-Host "  Customer 360 - Production Installation (6:00 PM Schedule)"
Write-Host "=" * 70
Write-Host ""

# ============================================================
# STEP 1: Verify Prerequisites
# ============================================================
Write-Host "[1/8] Checking prerequisites..."

# Check Python
try {
    $PythonVersion = python --version 2>&1
    if ($PythonVersion -match "3\.(10|11|12)") {
        Write-Host "  OK: Python $PythonVersion"
    } else {
        Write-Host "  WARNING: Python 3.10+ recommended. Found: $PythonVersion"
    }
} catch {
    Write-Host "  ERROR: Python not found. Please install Python 3.10 or 3.11"
    exit 1
}

# Check SQL Server connectivity
try {
    $Result = sqlcmd -S DBUATL01 -d Customer_FeedBack_JIT -Q "SELECT 1" -h -1 -W 2>$null
    if ($Result -eq "1") {
        Write-Host "  OK: SQL Server connection (DBUATL01)"
    } else {
        Write-Host "  WARNING: Cannot connect to DBUATL01"
    }
} catch {
    Write-Host "  WARNING: sqlcmd not found. SQL Server tools may not be installed."
}

# Check .env file
$EnvFile = "$InstallDir\config\.env"
if (Test-Path $EnvFile) {
    Write-Host "  OK: Configuration file exists"
} else {
    Write-Host "  ERROR: Configuration file not found: $EnvFile"
    Write-Host "         Copy config\.env.production to config\.env and update settings"
    exit 1
}

# ============================================================
# STEP 2: Create Virtual Environment
# ============================================================
Write-Host "[2/8] Creating Python virtual environment..."

if (Test-Path "$InstallDir\venv") {
    $Answer = Read-Host "  Virtual environment exists. Recreate? (y/N)"
    if ($Answer -eq "y") {
        Remove-Item "$InstallDir\venv" -Recurse -Force
        python -m venv "$InstallDir\venv"
        Write-Host "  OK: Virtual environment recreated"
    } else {
        Write-Host "  OK: Using existing virtual environment"
    }
} else {
    python -m venv "$InstallDir\venv"
    Write-Host "  OK: Virtual environment created"
}

# ============================================================
# STEP 3: Install Python Dependencies
# ============================================================
Write-Host "[3/8] Installing Python dependencies..."

& "$InstallDir\venv\Scripts\pip.exe" install --upgrade pip | Out-Null
& "$InstallDir\venv\Scripts\pip.exe" install -r "$InstallDir\app\requirements.txt" | Out-Null

Write-Host "  OK: Dependencies installed"

# Verify key packages
$Packages = @("pyodbc", "openai", "chromadb", "fastapi", "uvicorn")
foreach ($pkg in $Packages) {
    try {
        & "$InstallDir\venv\Scripts\python.exe" -c "import $pkg" 2>$null
        Write-Host "    - $pkg : OK"
    } catch {
        Write-Host "    - $pkg : FAILED"
    }
}

# ============================================================
# STEP 4: Set Environment Variables
# ============================================================
Write-Host "[4/8] Setting environment variables..."

# Check if Oracle password is set
$OraclePwd = [Environment]::GetEnvironmentVariable("ORACLE_DB_PASSWORD", "User")
if (-not $OraclePwd) {
    Write-Host "  WARNING: ORACLE_DB_PASSWORD not set"
    Write-Host "          oracle_db_util.py will not work without this"
    Write-Host "          Set it with: [Environment]::SetEnvironmentVariable('ORACLE_DB_PASSWORD', 'your-password', 'User')"
}

# ============================================================
# STEP 5: Verify ChromaDB
# ============================================================
Write-Host "[5/8] Verifying ChromaDB vector store..."

$ChromaPath = "$InstallDir\VIBO\chromadb"
if (Test-Path $ChromaPath) {
    $ChromaSize = (Get-ChildItem $ChromaPath -Recurse | Measure-Object -Property Length -Sum).Sum / 1MB
    Write-Host "  OK: ChromaDB found ($([math]::Round($ChromaSize, 2)) MB)"
} else {
    Write-Host "  WARNING: ChromaDB not found at $ChromaPath"
    Write-Host "          You'll need to run vibo_embedding_pipeline.py to create embeddings"
}

# ============================================================
# STEP 6: Test Database Connection
# ============================================================
Write-Host "[6/8] Testing database connection..."

try {
    & "$InstallDir\venv\Scripts\python.exe" -c "
import sys
sys.path.insert(0, '$InstallDir\VIBO')
from vibo_database import test_connection
result = test_connection()
if result['status'] == 'connected':
    print('OK: Connected to', result['database'])
else:
    print('ERROR:', result.get('error'))
    sys.exit(1)
" 2>&1 | Write-Host "  $_"
} catch {
    Write-Host "  WARNING: Database test failed"
}

# ============================================================
# STEP 7: Create Scheduled Tasks (6:00 PM)
# ============================================================
Write-Host "[7/8] Setting up scheduled tasks (6:00 PM schedule)..."

$Answer = Read-Host "  Install Daily Pipeline scheduled task? (6:00 PM daily) (Y/n)"
if ($Answer -ne "n") {
    $Action = New-ScheduledTaskAction `
        -Execute "$InstallDir\venv\Scripts\python.exe" `
        -Argument "$InstallDir\app\run_daily_pipeline.py" `
        -WorkingDirectory $InstallDir

    $Trigger = New-ScheduledTaskTrigger -Daily -At 6pm

    $Principal = New-ScheduledTaskPrincipal `
        -UserId "SYSTEM" `
        -LogonType ServiceAccount `
        -RunLevel Highest

    Register-ScheduledTask `
        -TaskName "Customer360-DailyPipeline" `
        -Action $Action `
        -Trigger $Trigger `
        -Principal $Principal `
        -Description "Customer 360 Daily ETL Pipeline (runs at 6:00 PM)" `
        -Force | Out-Null

    Write-Host "  OK: Scheduled task 'Customer360-DailyPipeline' installed (6:00 PM)"
}

$Answer = Read-Host "  Install VIBO API as auto-start service? (Y/n)"
if ($Answer -ne "n") {
    $Action = New-ScheduledTaskAction `
        -Execute "$InstallDir\venv\Scripts\python.exe" `
        -Argument "$InstallDir\VIBO\vibo_api.py" `
        -WorkingDirectory "$InstallDir\VIBO"

    $Trigger = New-ScheduledTaskTrigger -AtStartup

    Register-ScheduledTask `
        -TaskName "Customer360-VIBO-API" `
        -Action $Action `
        -Trigger $Trigger `
        -Principal $Principal `
        -Description "Customer 360 VIBO Chatbot API Server" `
        -Force | Out-Null

    Write-Host "  OK: Scheduled task 'Customer360-VIBO-API' installed"
}

# ============================================================
# STEP 8: Final Verification
# ============================================================
Write-Host "[8/8] Running final verification..."

Write-Host ""
Write-Host "Installation Summary:"
Write-Host "  Install Directory: $InstallDir"
Write-Host "  Python:            $PythonVersion"
Write-Host "  Virtual Env:       $InstallDir\venv"
Write-Host "  Config:            $EnvFile"
Write-Host "  Logs:              $InstallDir\logs"
Write-Host ""
Write-Host "Scheduled Tasks:"
Get-ScheduledTask -TaskName "Customer360-*" -ErrorAction SilentlyContinue | ForEach-Object {
    $Triggers = $_.Triggers
    if ($Triggers) {
        $Time = if ($Triggers.StartBoundary) { [DateTime]::Parse($Triggers.StartBoundary).ToString("HH:mm") } else { "Startup" }
        Write-Host "  - $($_.TaskName) at $Time"
    } else {
        Write-Host "  - $($_.TaskName)"
    }
}
Write-Host ""

Write-Host "=" * 70
Write-Host "  INSTALLATION COMPLETE"
Write-Host "=" * 70
Write-Host ""
Write-Host "Next Steps:"
Write-Host "  1. Update config\.env with production settings"
Write-Host "  2. Set ORACLE_DB_PASSWORD environment variable (if needed)"
Write-Host "  3. Run: $InstallDir\scripts\check_pipeline_status.ps1"
Write-Host "  4. Start VIBO API: Start-ScheduledTask -TaskName 'Customer360-VIBO-API'"
Write-Host "  5. Test API: Invoke-RestMethod http://localhost:8000/health"
Write-Host ""
Write-Host "For monitoring:"
Write-Host "  Status Check:  $InstallDir\scripts\check_pipeline_status.ps1"
Write-Host "  Logs:         $InstallDir\logs\"
Write-Host "=" * 70
