# Customer 360 - Production Deployment Guide

## Overview

This guide covers deploying the Customer 360 system to a production Windows Server.

---

## Prerequisites

### Server Requirements
- **OS**: Windows Server 2019 or later
- **Python**: 3.10 or 3.11
- **Database Access**: SQL Server connectivity to DBUATL01
- **Network**: Access to Azure OpenAI API
- **Disk Space**: At least 10 GB free

### Required Access
- SQL Server database access (Windows Authentication)
- Azure OpenAI API key and endpoint
- Oracle CRM database access (optional, for oracle_db_util.py)

---

## Deployment Package Contents

```
Customer360-Production/
├── app/
│   ├── llm_summariser_v4.py       # LLM summarizer
│   ├── llm_enrichment.py           # Enrichment calculations
│   ├── load_transcripts_v2.py      # Call transcript loader
│   ├── refresh_revenue_cache.py    # Revenue cache refresh
│   ├── refresh_device_assets.py    # Device assets refresh
│   ├── oracle_db_util.py           # Oracle CRM connection (optional)
│   └── run_daily_pipeline.py       # Main pipeline orchestrator
│
├── VIBO/
│   ├── vibo_api.py                 # FastAPI chatbot server
│   ├── vibo_llm.py                 # LLM client
│   ├── vibo_vector_store.py        # ChromaDB wrapper
│   ├── vibo_database.py            # Database connection
│   ├── vibo_sql_tools.py           # SQL query tools
│   ├── vibo_config.py              # Configuration
│   ├── vibo_embedding_pipeline.py  # Embedding refresh
│   ├── vibo_daily_pipeline.py      # VIBO daily update
│   └── chromadb/                   # Vector database (453 MB)
│
├── scripts/
│   ├── install_production.ps1      # Installation script
│   ├── install_scheduled_task.ps1  # Windows Task Scheduler setup
│   ├── install_vibo_service.ps1    # VIBO API as Windows Service
│   └── check_pipeline_status.ps1   # Status monitoring
│
├── config/
│   └── .env.production             # Production environment template
│
├── logs/                           # Log directory
├── requirements.txt                # Python dependencies
└── README_DEPLOYMENT.md            # This file
```

---

## Step 1: Prepare Production Server

### 1.1 Install Python
```powershell
# Download Python 3.11 from python.org
# Install to: C:\Python311
# Add to PATH
```

### 1.2 Create Directory Structure
```powershell
New-Item -Path "C:\Customer360" -ItemType Directory -Force
New-Item -Path "C:\Customer360\app" -ItemType Directory -Force
New-Item -Path "C:\Customer360\VIBO" -ItemType Directory -Force
New-Item -Path "C:\Customer360\config" -ItemType Directory -Force
New-Item -Path "C:\Customer360\logs" -ItemType Directory -Force
New-Item -Path "C:\Customer360\scripts" -ItemType Directory -Force
```

---

## Step 2: Deploy Application Files

### 2.1 Copy Files from Dev Server
```powershell
# From development machine, run:
# (Replace PRODSERVER with your production server name)

# Main application files
Copy-Item `
    "llm_summariser_v4.py",`
    "llm_enrichment.py",`
    "load_transcripts_v2.py",`
    "refresh_revenue_cache.py",`
    "refresh_device_assets.py",`
    "oracle_db_util.py",`
    "run_daily_pipeline.py",`
    "requirements.txt" `
    -Destination "\\PRODSERVER\C$\Customer360\app\"

# VIBO files
Copy-Item "VIBO\*.py" -Destination "\\PRODSERVER\C$\Customer360\VIBO\" -Exclude "test_*", "check_*", "monitor_*"

# ChromaDB (important - copy entire directory)
Robocopy "VIBO\chromadb" "\\PRODSERVER\C$\Customer360\VIBO\chromadb" /E /Z

# Scripts
Copy-Item "scripts\*.ps1" -Destination "\\PRODSERVER\C$\Customer360\scripts\"

# Config template
Copy-Item ".env" "\\PRODSERVER\C$\Customer360\config\.env.production"
```

### 2.2 Run Deployment Script (Alternative)
See `scripts\create_deployment_package.ps1` (created separately)

---

## Step 3: Configure Production Environment

### 3.1 Create .env File
On production server, create `C:\Customer360\config\.env`:

```bash
# ============================================================
# CUSTOMER 360 - PRODUCTION CONFIGURATION
# ============================================================

# ------------------------------------------------------------
# AZURE OPENAI (LLM + Embeddings)
# ------------------------------------------------------------
AZURE_OPENAI_API_KEY=your-production-key-here
AZURE_OPENAI_ENDPOINT=https://vmiearchdemo.openai.azure.com/
AZURE_OPENAI_DEPLOYMENT=gpt-4o
AZURE_OPENAI_API_VERSION=2024-02-15-preview

# ------------------------------------------------------------
# DATABASE (SQL Server)
# ------------------------------------------------------------
VIBO_DB_SERVER=DBUATL01
VIBO_DB_NAME=Customer_FeedBack_JIT
VIBO_DB_DRIVER={ODBC Driver 17 for SQL Server}
VIBO_DB_TRUSTED=yes

# ------------------------------------------------------------
# VIBO SETTINGS
# ------------------------------------------------------------
VIBO_LLM_PROVIDER=azure_openai
VIBO_EMBEDDING_PROVIDER=azure_openai
VIBO_AZURE_EMBED_DEPLOYMENT=text-embedding-3-small
VIBO_CHROMA_PATH=C:\Customer360\VIBO\chromadb
VIBO_EMBED_BATCH_SIZE=20
VIBO_VECTOR_TOP_K=5

# ------------------------------------------------------------
# ORACLE DATABASE (Optional - for oracle_db_util.py)
# ------------------------------------------------------------
ORACLE_DB_HOST=ora01-primary-vip.prod.vmie.local
ORACLE_DB_PORT=1521
ORACLE_DB_SERVICE_NAME=CRMPROD
ORACLE_DB_USER=SUPER
# ORACLE_DB_PASSWORD - Set as environment variable for security
```

### 3.2 Set Environment Variable for Oracle Password
```powershell
# Set as user or system environment variable
[Environment]::SetEnvironmentVariable("ORACLE_DB_PASSWORD", "your-password", "User")
```

---

## Step 4: Install Python Dependencies

On production server:

```powershell
# Create virtual environment
cd C:\Customer360
python -m venv venv

# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt

# Verify installation
python -c "import pyodbc, openai, chromadb, fastapi; print('All dependencies OK')"
```

---

## Step 5: Set Up Scheduled Tasks

### 5.1 Daily Pipeline Schedule
Runs at 6:00 AM daily (after ETL completes)

```powershell
.\scripts\install_scheduled_task.ps1
```

Or manually:
```powershell
$action = New-ScheduledTaskAction `
    -Execute "C:\Customer360\venv\Scripts\python.exe" `
    -Argument "C:\Customer360\app\run_daily_pipeline.py" `
    -WorkingDirectory "C:\Customer360"

$trigger = New-ScheduledTaskTrigger -Daily -At 6am

$principal = New-ScheduledTaskPrincipal `
    -UserId "DOMAIN\ServiceAccount" `
    -LogonType Password `
    -RunLevel Highest

Register-ScheduledTask `
    -TaskName "Customer360-DailyPipeline" `
    -Action $action `
    -Trigger $trigger `
    -Principal $principal `
    -Description "Customer 360 Daily ETL Pipeline"
```

### 5.2 VIBO Daily Update (Optional)
If running VIBO updates separately from main pipeline:

```powershell
$action = New-ScheduledTaskAction `
    -Execute "C:\Customer360\venv\Scripts\python.exe" `
    -Argument "C:\Customer360\VIBO\vibo_daily_pipeline.py" `
    -WorkingDirectory "C:\Customer360\VIBO"

$trigger = New-ScheduledTaskTrigger -Daily -At 7am

Register-ScheduledTask `
    -TaskName "Customer360-VIBO-Update" `
    -Action $action `
    -Trigger $trigger
```

---

## Step 6: Deploy VIBO API Server

### Option A: Run as Windows Service (Recommended)
```powershell
.\scripts\install_vibo_service.ps1
```

### Option B: Run as Scheduled Task (Auto-start)
```powershell
$action = New-ScheduledTaskAction `
    -Execute "C:\Customer360\venv\Scripts\python.exe" `
    -Argument "C:\Customer360\VIBO\vibo_api.py" `
    -WorkingDirectory "C:\Customer360\VIBO"

$trigger = New-ScheduledTaskTrigger -AtStartup

Register-ScheduledTask `
    -TaskName "Customer360-VIBO-API" `
    -Action $action `
    -Trigger $trigger `
    -RunLevel Highest
```

### Option C: Run Manually (For Testing)
```powershell
cd C:\Customer360\VIBO
..\venv\Scripts\python.exe vibo_api.py
```

---

## Step 7: Verify Deployment

### 7.1 Test Database Connection
```powershell
cd C:\Customer360\VIBO
..\venv\Scripts\python.exe -c "from vibo_database import test_connection; print(test_connection())"
```

### 7.2 Test ChromaDB
```powershell
..\venv\Scripts\python.exe -c "from vibo_vector_store import VectorStore; s=VectorStore(); s.initialize(); print(s.get_stats())"
```

### 7.3 Test API
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/health" | ConvertTo-Json
```

### 7.4 Run Status Check
```powershell
.\scripts\check_pipeline_status.ps1
```

---

## Step 8: Monitoring

### Log Files
| Component | Log Location |
|-----------|--------------|
| Daily Pipeline | `C:\Customer360\logs\daily_pipeline.log` |
| VIBO API | `C:\Customer360\VIBO\vibo_api.log` |
| VIBO Embeddings | `C:\Customer360\VIBO\vibo_embedding.log` |

### Status Checks
```powershell
# Quick status check
C:\Customer360\scripts\check_pipeline_status.ps1

# Check scheduled tasks
Get-ScheduledTask -TaskName "Customer360-*"

# Check VIBO API service
Get-Service -Name "VIBOAPI"
```

---

## Rollback Procedure

If issues occur:

```powershell
# 1. Stop scheduled tasks
Disable-ScheduledTask -TaskName "Customer360-*"

# 2. Stop VIBO API
Stop-Service -Name "VIBOAPI" -Force

# 3. Restore previous version
Copy-Item "\\BACKUPSERVER\Customer360\backup\*" "C:\Customer360\" -Recurse -Force

# 4. Restart services
Enable-ScheduledTask -TaskName "Customer360-DailyPipeline"
Start-Service -Name "VIBOAPI"
```

---

## Troubleshooting

### Issue: "Module not found"
**Solution**: Ensure virtual environment is activated and dependencies installed:
```powershell
C:\Customer360\venv\Scripts\Activate.ps1
pip install -r C:\Customer360\requirements.txt
```

### Issue: "Database connection failed"
**Solution**: Verify SQL Server connectivity and Windows Authentication:
```powershell
Test-NetConnection -ComputerName DBUATL01 -Port 1433
```

### Issue: "ChromaDB not found"
**Solution**: Ensure chromadb directory was copied:
```powershell
Test-Path "C:\Customer360\VIBO\chromadb"
```

### Issue: "Azure OpenAI API error"
**Solution**: Verify API key and endpoint in .env file

---

## Security Checklist

- [ ] Oracle database password stored as environment variable (not in .env)
- [ ] Azure OpenAI API key secured (restrict access to production server only)
- [ ] Service account has minimum required permissions
- [ ] Log files directory has appropriate permissions
- [ ] Firewall rules configured for port 8000 (VIBO API)
- [ ] Backups scheduled for ChromaDB directory

---

## Contacts

| Issue | Contact |
|-------|---------|
| Database access | DBA Team |
| Azure OpenAI access | Cloud Team |
| Server access | Infrastructure Team |
| Application issues | Development Team |
