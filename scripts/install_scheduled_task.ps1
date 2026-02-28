# VIBO Scheduled Task Installer
# ============================
# This script creates a Windows Scheduled Task for daily VIBO incremental updates
#
# Usage: Run as Administrator
#   .\install_scheduled_task.ps1

$ErrorActionPreference = "Stop"

# Configuration
$TaskName = "VIBO Daily Incremental Update"
$BatchFile = "C:\Projects\Customer360\VIBO\vibo_daily_update.bat"
$LogDir = "Y:\AI_DATA\logs\vibo"

# Ensure log directory exists
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
    Write-Host "Created log directory: $LogDir" -ForegroundColor Green
}

# Check if batch file exists
if (-not (Test-Path $BatchFile)) {
    Write-Host "ERROR: Batch file not found: $BatchFile" -ForegroundColor Red
    exit 1
}

Write-Host "`n===========================================" -ForegroundColor Cyan
Write-Host "VIBO Scheduled Task Installer" -ForegroundColor Cyan
Write-Host "===========================================`n" -ForegroundColor Cyan

# Remove existing task if it exists
$existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existingTask) {
    Write-Host "Removing existing task..." -ForegroundColor Yellow
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Start-Sleep -Seconds 1
}

# Create the scheduled task action
$action = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument "/c ""$BatchFile""" `
    -WorkingDirectory "C:\Projects\Customer360"

# Create trigger - Daily at 2:00 AM
$trigger = New-ScheduledTaskTrigger -Daily -At "2:00AM"

# Create principal - Run with highest privileges
$principal = New-ScheduledTaskPrincipal `
    -UserId "SYSTEM" `
    -LogonType ServiceAccount `
    -RunLevel Highest

# Create settings
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 5)

# Register the scheduled task
Write-Host "Creating scheduled task..." -ForegroundColor Green
Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Principal $principal `
    -Settings $settings `
    -Description "VIBO (Virtual Intelligence Briefing Officer) Daily Incremental Update. Embeds new customer transcripts and events into ChromaDB vector store. Runs in incremental mode - only processes new data since last run. Estimated time: 2-5 minutes. No downtime." `
    -ErrorAction Stop

Write-Host "`n===========================================" -ForegroundColor Green
Write-Host "SUCCESS: Task created!" -ForegroundColor Green
Write-Host "===========================================`n" -ForegroundColor Green

# Display task info
$task = Get-ScheduledTask -TaskName $TaskName
Write-Host "Task Name:     " -NoNewline; Write-Host $task.TaskName -ForegroundColor Cyan
Write-Host "Description:   " -NoNewline; Write-Host $task.Description -ForegroundColor Cyan
Write-Host "Trigger:       " -NoNewline; Write-Host "Daily at 2:00 AM" -ForegroundColor Cyan
Write-Host "Run As:        " -NoNewline; Write-Host $task.Principal.UserId -ForegroundColor Cyan
Write-Host "Script:        " -NoNewline; Write-Host $BatchFile -ForegroundColor Cyan
Write-Host "Log Directory: " -NoNewline; Write-Host $LogDir -ForegroundColor Cyan

Write-Host "`nCommands to manage the task:" -ForegroundColor Yellow
Write-Host "  View task:        Get-ScheduledTask -TaskName '$TaskName'" -ForegroundColor White
Write-Host "  Run manually:     Start-ScheduledTask -TaskName '$TaskName'" -ForegroundColor White
Write-Host "  Disable:          Disable-ScheduledTask -TaskName '$TaskName'" -ForegroundColor White
Write-Host "  Enable:           Enable-ScheduledTask -TaskName '$TaskName'" -ForegroundColor White
Write-Host "  Delete:           Unregister-ScheduledTask -TaskName '$TaskName'" -ForegroundColor White
Write-Host "  View task history: Get-ScheduledTaskInfo -TaskName '$TaskName'" -ForegroundColor White

Write-Host "`nTo view logs:" -ForegroundColor Yellow
Write-Host "  Get-Content '$LogDir\vibo_daily_*.log' -Tail 50" -ForegroundColor White

Write-Host "`n===========================================`n" -ForegroundColor Cyan
