# Setup Automated Progress Monitoring
# Runs progress check every 30 minutes and logs to Y:\AI_DATA\logs\vibo\

$ErrorActionPreference = "Stop"

$TaskName = "VIBO Progress Monitor"
$BatchFile = "C:\Projects\Customer360\VIBO\log_progress.bat"
$LogDir = "C:\Projects\Customer360\VIBO\logs"

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host " VIBO Automated Monitoring Setup" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# Ensure log directory exists
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
    Write-Host "Created log directory: $LogDir" -ForegroundColor Green
}

# Remove existing task if it exists
$existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existingTask) {
    Write-Host "Removing existing monitoring task..." -ForegroundColor Yellow
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Start-Sleep -Seconds 1
}

# Create the scheduled task
$action = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument "/c ""$BatchFile""" `
    -WorkingDirectory "C:\Projects\Customer360"

# Trigger daily - repeat every 30 minutes for 24 hours
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes 30) -RepetitionDuration (New-TimeSpan -Days 1)

$principal = New-ScheduledTaskPrincipal `
    -UserId $env:USERNAME `
    -LogonType Interactive `
    -RunLevel Highest

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable

Write-Host "Creating scheduled task..." -ForegroundColor Green
Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Principal $principal `
    -Settings $settings `
    -Description "Logs VIBO progress every 30 minutes to $LogDir" `
    -ErrorAction Stop

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "SUCCESS: Monitoring task created!" -ForegroundColor Green
Write-Host "========================================`n" -ForegroundColor Green

Write-Host "Task Name:     $TaskName" -ForegroundColor Cyan
Write-Host "Runs:          Every 30 minutes" -ForegroundColor Cyan
Write-Host "Logs written:  $LogDir" -ForegroundColor Cyan
Write-Host ""
Write-Host "To view logs:" -ForegroundColor Yellow
Write-Host "  Get-Content '$LogDir\progress_*.txt' -Tail 50" -ForegroundColor White
Write-Host "  Or: Get-Content 'C:\Projects\Customer360\VIBO\logs\progress_*.txt' -Tail 50" -ForegroundColor White
Write-Host ""
Write-Host "To stop monitoring:" -ForegroundColor Yellow
Write-Host "  Unregister-ScheduledTask -TaskName '$TaskName' -Confirm:`$false" -ForegroundColor White
Write-Host ""
Write-Host "========================================`n" -ForegroundColor Cyan
