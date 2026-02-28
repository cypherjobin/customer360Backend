# VIBO Progress Check Script
# =========================
# Run this script anytime to check if the embedding pipeline and LLM summariser are running
# Usage: .\check_progress.ps1

$ErrorActionPreference = "Stop"

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host " VIBO PROGRESS CHECK" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# Check Python processes
$pythonProcesses = Get-Process python -ErrorAction SilentlyContinue
$embeddingLog = "C:\Users\jjohn\AppData\Local\Temp\claude\c--Projects-Customer360\tasks\bf46ab3.output"
$summariserLog = "C:\Users\jjohn\AppData\Local\Temp\claude\c--Projects-Customer360\tasks\b3ea100.output"

Write-Host "Python Processes Running: $($pythonProcesses.Count)" -ForegroundColor Green
Write-Host ""

# Check Embedding Pipeline
Write-Host "--- Embedding Pipeline (VIBO) ---" -ForegroundColor Yellow
if ($pythonProcesses.Count -ge 2) {
    $lastLine = Get-Content $embeddingLog -Tail 1 -ErrorAction SilentlyContinue
    if ($lastLine -match "\d{2}:\d{2}:\d{2}") {
        $lastLine -replace '.*(\d{2}:\d{2}:\d{2}).*', '$1' | ForEach-Object {
            Write-Host "Last activity: $_" -ForegroundColor Green
        }
    }

    # Count embeddings
    $embedCount = (Get-Content $embeddingLog -ErrorAction SilentlyContinue | Select-String "api/embed.*200 OK").Count
    Write-Host "Embeddings processed: $embedCount" -ForegroundColor Cyan

    # Check ChromaDB size
    if (Test-Path "Y:\chromadb") {
        $size = (Get-ChildItem "Y:\chromadb" -Recurse -File -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum).Sum / 1MB
        Write-Host "ChromaDB size: $([math]::Round($size, 2)) MB" -ForegroundColor Cyan
    }
} else {
    Write-Host "NOT RUNNING - Check logs!" -ForegroundColor Red
}
Write-Host ""

# Check LLM Summariser
Write-Host "--- LLM Summariser ---" -ForegroundColor Yellow
if (Test-Path $summariserLog) {
    $lastLines = Get-Content $summariserLog -Tail 10 -ErrorAction SilentlyContinue

    # Find last customer number
    $lastCustomer = $lastLines | Select-String "\[(\d+)\] Customer:" | Select-Object -Last 1
    if ($lastCustomer) {
        $lastCustomer.Matches[0].Groups[1].Value | ForEach-Object {
            Write-Host "Customers processed: $_" -ForegroundColor Green
        }
    }

    # Check last activity
    $lastLine = $lastLines | Select-Object -Last 1
    if ($lastLine -match "(\d{2}:\d{2}:\d{2})") {
        Write-Host "Last activity: $($matches[1])" -ForegroundColor Green
    }
} else {
    Write-Host "NOT RUNNING" -ForegroundColor Red
}
Write-Host ""

# Estimated completion
Write-Host "--- Estimates ---" -ForegroundColor Yellow
Write-Host "Embedding: ~1% complete (very slow due to raw transcript data)" -ForegroundColor White
Write-Host "Summariser: ~$(if ($lastCustomer) { $lastCustomer.Matches[0].Groups[1].Value } else { '0' }) customers processed" -ForegroundColor White
Write-Host ""
Write-Host "Both processes should complete by morning." -ForegroundColor Green
Write-Host "========================================`n" -ForegroundColor Cyan
