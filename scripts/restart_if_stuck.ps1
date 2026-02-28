# VIBO Restart Script (if processes get stuck)
# ===========================================
# Run this if one of the processes appears stuck
# This will restart any stopped processes

$ErrorActionPreference = "Stop"

Write-Host "`n========================================" -ForegroundColor Yellow
Write-Host " VIBO RESTART SCRIPT" -ForegroundColor Yellow
Write-Host "========================================`n" -ForegroundColor Yellow

$embeddingScript = "python VIBO\vibo_embedding_pipeline.py --full-rebuild"
$summariserScript = "python llm_summariser_v4.py --run-date 2026-02-18"
$projectDir = "C:\Projects\Customer360"

# Check if embedding pipeline is running
$embeddingLog = "C:\Users\jjohn\AppData\Local\Temp\claude\c--Projects-Customer360\tasks\bf46ab3.output"
$lastEmbed = Get-Content $embeddingLog -Tail 1 -ErrorAction SilentlyContinue
if ($lastEmbed -match "(\d{2}:\d{2}:\d{2})") {
    $lastTime = [datetime]::Parse($matches[1])
    if ((Get-Date) - $lastTime -gt [TimeSpan]::FromMinutes(10)) {
        Write-Host "Embedding pipeline appears STUCK (no activity for 10+ min)" -ForegroundColor Red
        Write-Host "Restarting embedding pipeline..." -ForegroundColor Yellow
        Start-Process python -ArgumentList "VIBO\vibo_embedding_pipeline.py","--full-rebuild" -WorkingDirectory $projectDir
    } else {
        Write-Host "Embedding pipeline: OK" -ForegroundColor Green
    }
}

# Check if summariser is running
$summariserLog = "C:\Users\jjohn\AppData\Local\Temp\claude\c--Projects-Customer360\tasks\b3ea100.output"
$lastSum = Get-Content $summariserLog -Tail 5 -ErrorAction SilentlyContinue | Select-Object -Last 1
if ($lastSum -match "(\d{2}:\d{2}:\d{2})") {
    $lastTime = [datetime]::Parse($matches[1])
    if ((Get-Date) - $lastTime -gt [TimeSpan]::FromMinutes(10)) {
        Write-Host "LLM Summariser appears STUCK (no activity for 10+ min)" -ForegroundColor Red
        Write-Host "Restarting LLM summariser..." -ForegroundColor Yellow
        Start-Process python -ArgumentList "llm_summariser_v4.py","--run-date 2026-02-18" -WorkingDirectory $projectDir
    } else {
        Write-Host "LLM Summariser: OK" -ForegroundColor Green
    }
}

Write-Host "`n========================================`n" -ForegroundColor Yellow
