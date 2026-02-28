# Customer 360 Pipeline Status Check
# =================================
# Shows current status of all pipeline components
# Usage: .\check_pipeline_status.ps1

$ErrorActionPreference = "Stop"

# Database connection info
$Server = "DBUATL01"
$Database = "Customer_FeedBack_JIT"
$LogFile = "C:\Projects\Customer360\daily_pipeline.log"

Write-Host "`n" -NoNewline
for ($i = 0; $i -lt 60; $i++) { Write-Host "=" -NoNewline -ForegroundColor Cyan }
Write-Host "`n CUSTOMER 360 - PIPELINE STATUS CHECK" -ForegroundColor Cyan
for ($i = 0; $i -lt 60; $i++) { Write-Host "=" -NoNewline -ForegroundColor Cyan }
Write-Host "`n" -ForegroundColor Cyan

# ============================================================
# 1. CHECK PIPELINE LOG
# ============================================================
Write-Host "`n[1] DAILY PIPELINE LOG" -ForegroundColor Yellow
Write-Host "-" * 40 -ForegroundColor DarkGray

if (Test-Path $LogFile) {
    $lastLines = Get-Content $LogFile -Tail 10

    # Find last completion message
    $completion = Get-Content $LogFile | Select-String "PIPELINE SUMMARY|Completed At:" | Select-Object -Last 2
    if ($completion) {
        $lastCompletion = $completion | Select-Object -Last 1
        if ($lastCompletion -match "(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})") {
            Write-Host "Last Completed: " -NoNewline
            Write-Host $matches[1] -ForegroundColor Green
        }
    }

    # Check if currently running
    $running = Get-Content $LogFile | Select-String "STEP [0-9]:" | Select-Object -Last 1
    if ($running) {
        $currentStep = $running.Line.Trim()
        $lastLineTime = ($lastLines | Select-Object -Last 1) -replace '.*?(\d{2}:\d{2}:\d{2}).*', '$1'
        Write-Host "Current Step: " -NoNewline
        Write-Host $currentStep -ForegroundColor Cyan
        Write-Host "Last Activity: " -NoNewline
        Write-Host $lastLineTime -ForegroundColor Gray
    }
} else {
    Write-Host "Log file not found: $LogFile" -ForegroundColor Red
}

# ============================================================
# 2. DATABASE QUERIES
# ============================================================
Write-Host "`n[2] DATABASE STATUS" -ForegroundColor Yellow
Write-Host "-" * 40 -ForegroundColor DarkGray

function Run-Query {
    param($Query)
    $result = sqlcmd -S $Server -d $Database -Q $Query -W -h -1 -s "," 2>&1
    if ($LASTEXITCODE -eq 0) {
        return $result
    }
    return $null
}

# Event counts by source
$eventsQuery = @"
SELECT source_system, COUNT(*) as count
FROM Customer360_Events
GROUP BY source_system
ORDER BY source_system
"@
$events = Run-Query $eventsQuery
if ($events) {
    Write-Host "`nCustomer360_Events (by source):" -ForegroundColor White
    $events | ForEach-Object {
        if ($_ -match ",(\d+)$") {
            $parts = $_ -split ","
            Write-Host "  $($parts[0].PadRight(20)): " -NoNewline
            Write-Host $parts[1] -ForegroundColor Cyan
        }
    }
}

# LLM Summary status
$summaryQuery = @"
SELECT summary_status, COUNT(*) as count
FROM vw_CustomersPendingSummary
GROUP BY summary_status
ORDER BY count DESC
"@
$summaries = Run-Query $summaryQuery
if ($summaries) {
    Write-Host "`nLLM Summary Status:" -ForegroundColor White
    $summaries | ForEach-Object {
        if ($_ -match ",(\d+)$") {
            $parts = $_ -split ","
            $status = $parts[0]
            $count = $parts[1]
            $color = if ($status -eq "UP_TO_DATE") { "Green" }
                     elseif ($status -eq "EVENTS_UPDATED") { "Yellow" }
                     else { "Red" }
            Write-Host "  $status".PadRight(25) -NoNewline -ForegroundColor White
            Write-Host ": $count" -ForegroundColor $color
        }
    }
}

# VIBO Embedding status
$embeddingQuery = @"
SELECT TOP 1
    run_date,
    source_table,
    watermark_timestamp,
    records_processed,
    status
FROM VIBO_Embedding_Log
ORDER BY run_date DESC
"@
$embedding = Run-Query $embeddingQuery
if ($embedding -match ",") {
    $parts = $embedding -split ","
    Write-Host "`nVIBO Embedding Log (latest):" -ForegroundColor White
    Write-Host "  Run Date      : " -NoNewline
    Write-Host $parts[0] -ForegroundColor Cyan
    Write-Host "  Source        : " -NoNewline
    Write-Host $parts[1] -ForegroundColor Cyan
    Write-Host "  Watermark     : " -NoNewline
    Write-Host $parts[2] -ForegroundColor Gray
    Write-Host "  Records       : " -NoNewline
    Write-Host $parts[3] -ForegroundColor Cyan
    Write-Host "  Status        : " -NoNewline
    $statusColor = if ($parts[4] -eq "COMPLETED") { "Green" } else { "Yellow" }
    Write-Host $parts[4] -ForegroundColor $statusColor
}

# Revenue Cache freshness
$revenueQuery = @"
SELECT
    MAX(cached_at) as last_updated,
    COUNT(DISTINCT customer_id) as total_customers
FROM Revenue_Cache
"@
$revenue = Run-Query $revenueQuery
if ($revenue) {
    $parts = ($revenue | Select-Object -First 1) -split ","
    Write-Host "`nRevenue Cache:" -ForegroundColor White
    Write-Host "  Last Updated  : " -NoNewline
    if ($parts[0]) { Write-Host $parts[0] -ForegroundColor Cyan }
    Write-Host "  Total Customers: " -NoNewline
    if ($parts[1]) { Write-Host $parts[1] -ForegroundColor Cyan }
}

# Device Assets freshness
$deviceQuery = @"
SELECT
    MAX(updated_at) as last_updated,
    COUNT(DISTINCT customer_id) as total_customers
FROM Customer_Device_Assets
"@
$device = Run-Query $deviceQuery
if ($device) {
    $parts = ($device | Select-Object -First 1) -split ","
    Write-Host "`nDevice Assets:" -ForegroundColor White
    Write-Host "  Last Updated  : " -NoNewline
    if ($parts[0]) { Write-Host $parts[0] -ForegroundColor Cyan }
    Write-Host "  Total Customers: " -NoNewline
    if ($parts[1]) { Write-Host $parts[1] -ForegroundColor Cyan }
}

# ============================================================
# 3. CHROMADB STATUS
# ============================================================
Write-Host "`n[3] CHROMADB VECTOR STORE" -ForegroundColor Yellow
Write-Host "-" * 40 -ForegroundColor DarkGray

$chromaPath = "C:\Projects\Customer360\VIBO\chromadb"
if (Test-Path $chromaPath) {
    $size = (Get-ChildItem $chromaPath -Recurse -File -ErrorAction SilentlyContinue |
             Measure-Object -Property Length -Sum).Sum / 1MB
    Write-Host "Location: $chromaPath" -ForegroundColor Gray
    Write-Host "Size     : " -NoNewline
    Write-Host "$([math]::Round($size, 2)) MB" -ForegroundColor Cyan
} else {
    Write-Host "ChromaDB not found at: $chromaPath" -ForegroundColor Red
}

# ============================================================
# 4. PYTHON PROCESSES
# ============================================================
Write-Host "`n[4] RUNNING PROCESSES" -ForegroundColor Yellow
Write-Host "-" * 40 -ForegroundColor DarkGray

$pythonProcs = Get-Process python -ErrorAction SilentlyContinue
if ($pythonProcs) {
    Write-Host "Python processes running: " -NoNewline
    Write-Host $pythonProcs.Count -ForegroundColor Cyan

    foreach ($proc in $pythonProcs) {
        $cmdLine = (Get-CimInstance Win32_Process -Filter "ProcessId = $($proc.Id)").CommandLine
        if ($cmdLine) {
            $shortCmd = if ($cmdLine.Length -gt 60) { $cmdLine.Substring(0, 60) + "..." } else { $cmdLine }
            Write-Host "  PID $($proc.Id): " -NoNewline -ForegroundColor Gray
            Write-Host $shortCmd -ForegroundColor White
        }
    }
} else {
    Write-Host "No Python processes running" -ForegroundColor Gray
}

# ============================================================
# 5. ACTION ITEMS
# ============================================================
Write-Host "`n[5] ACTION ITEMS" -ForegroundColor Yellow
Write-Host "-" * 40 -ForegroundColor DarkGray

$actionNeeded = $false

# Check for EVENTS_UPDATED
$pendingEventsQuery = @"
SELECT COUNT(*) as pending_count FROM vw_CustomersPendingSummary
WHERE summary_status = 'EVENTS_UPDATED'
"@
$pending = Run-Query $pendingEventsQuery
if ($pending) {
    $pendingCount = ($pending | Select-Object -First 1) -replace "\D", ""
    if ($pendingCount -match "^\d+$" -and [int]$pendingCount -gt 0) {
        Write-Host "[!] " -NoNewline -ForegroundColor Red
        Write-Host "$pendingCount customers have new events - LLM Summarizer needed" -ForegroundColor Yellow
        $actionNeeded = $true
    }
}

# Check if pipeline completed today
$today = Get-Date -Format "yyyy-MM-dd"
$todayCompletion = Get-Content $LogFile -ErrorAction SilentlyContinue | Select-String "Completed At:.*$today"
if (-not $todayCompletion) {
    Write-Host "[!] " -NoNewline -ForegroundColor Red
    Write-Host "Pipeline hasn't completed today yet" -ForegroundColor Yellow
    $actionNeeded = $true
}

if (-not $actionNeeded) {
    Write-Host "[OK] " -NoNewline -ForegroundColor Green
    Write-Host "All systems up to date" -ForegroundColor Green
}

Write-Host "`n" -NoNewline
for ($i = 0; $i -lt 60; $i++) { Write-Host "=" -NoNewline -ForegroundColor Cyan }
Write-Host "`n" -ForegroundColor Cyan
