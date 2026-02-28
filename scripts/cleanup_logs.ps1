# Cleanup old task logs to save space
# Run this anytime to free up disk space

$TempLogsPath = "C:\Users\jjohn\AppData\Local\Temp\claude\c--Projects-Customer360\tasks"

Write-Host "Checking disk space before cleanup..." -ForegroundColor Yellow
$before = (Get-PSDrive C).Free
Write-Host "Free space: $([math]::Round($before/1GB,2)) GB" -ForegroundColor Cyan

# Remove old logs (keep only currently running tasks)
Write-Host "`nRemoving old task logs..." -ForegroundColor Yellow
$runningTasks = @("bf46ab3", "b3ea100")  # Current embedding pipeline and summariser

Get-ChildItem $TempLogsPath -Filter "*.output" | ForEach-Object {
    $taskId = $_.Name -replace "\.output$"
    if ($taskId -notin $runningTasks) {
        Remove-Item $_.FullName -Force
        Write-Host "  Removed: $($_.Name)" -ForegroundColor Green
    }
}

Write-Host "`nCleanup complete!" -ForegroundColor Green
$after = (Get-PSDrive C).Free
Write-Host "Free space after: $([math]::Round($after/1GB,2)) GB" -ForegroundColor Cyan
Write-Host "Freed up: $([math]::Round(($after - $before)/1MB,2)) MB" -ForegroundColor Green
