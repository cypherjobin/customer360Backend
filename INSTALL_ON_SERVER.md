# Customer 360 - Server Installation Instructions

## Package Location
**Y:\Customer360-Deployments\Customer360-Production-20260220_153401.zip**
Size: 157.54 MB

---

## Installation Steps (Run on Production Server)

### Step 1: Extract the Package
Open PowerShell on the production server and run:

```powershell
# Create installation directory
New-Item -Path "C:\Customer360" -ItemType Directory -Force

# Extract package
Expand-Archive -Path "Y:\Customer360-Deployments\Customer360-Production-20260220_153401.zip" -DestinationPath "C:\Customer360" -Force
```

### Step 2: Copy .env Configuration
```powershell
# Copy config template and edit it
Copy-Item "C:\Customer360\config\.env.production" "C:\Customer360\config\.env"

# Edit with your production settings
notepad "C:\Customer360\config\.env"
```

**Update these values in .env:**
- `AZURE_OPENAI_API_KEY` - Your production key
- `AZURE_OPENAI_ENDPOINT` - Confirm endpoint URL
- `ORACLE_DB_PASSWORD` - Set as environment variable (see below)

### Step 3: Set Oracle Password Environment Variable
```powershell
[Environment]::SetEnvironmentVariable("ORACLE_DB_PASSWORD", "your-oracle-password", "User")
```

### Step 4: Run Installation Script
```powershell
cd C:\Customer360\scripts
.\install_production.ps1
```

This will:
- Create Python virtual environment
- Install all dependencies
- Create scheduled tasks
- Set up VIBO API service

### Step 5: Start VIBO API Server
```powershell
Start-ScheduledTask -TaskName "Customer360-VIBO-API"
```

### Step 6: Verify Installation
```powershell
# Check API health
Invoke-RestMethod -Uri "http://localhost:8000/health"

# Run full status check
C:\Customer360\scripts\check_pipeline_status.ps1
```

---

## Scheduled Tasks Created

| Task | Schedule | Description |
|------|----------|-------------|
| Customer360-DailyPipeline | 6:00 AM daily | Main ETL + LLM + Embeddings |
| Customer360-VIBO-API | At startup | Chatbot API server |

---

## Directory Structure After Installation

```
C:\Customer360\
├── app\                    # Pipeline scripts
├── VIBO\                   # Chatbot + ChromaDB
│   └── chromadb\           # Vector database (453 MB)
├── scripts\                # Installation & monitoring
├── config\                 # Configuration files
├── logs\                   # Log files
└── venv\                   # Python virtual environment
```

---

## Useful Commands

| Task | Command |
|------|---------|
| **Check Status** | `C:\Customer360\scripts\check_pipeline_status.ps1` |
| **View Pipeline Logs** | `Get-Content C:\Customer360\logs\daily_pipeline.log -Tail 50` |
| **View VIBO Logs** | `Get-Content C:\Customer360\VIBO\vibo_api.log -Tail 50` |
| **Restart VIBO API** | `Start-ScheduledTask -TaskName "Customer360-VIBO-API"` |
| **List Scheduled Tasks** | `Get-ScheduledTask -TaskName "Customer360-*"` |
| **Test Database** | `C:\Customer360\venv\Scripts\python.exe -c "from vibo_database import test_connection; print(test_connection())"` |

---

## API Endpoints (After Starting VIBO)

| Endpoint | Purpose |
|----------|---------|
| `http://localhost:8000/docs` | Interactive API documentation |
| `http://localhost:8000/health` | Health check |
| `http://localhost:8000/customer/{id}/summary` | Customer summary |
| `http://localhost:8000/customer/{id}/search?q=query` | Semantic search |
| `POST /customer/{id}/chat` | RAG-powered Q&A |

---

## Troubleshooting

### Issue: "Cannot connect to database"
**Solution**: Verify SQL Server connectivity and Windows Authentication

### Issue: "Azure OpenAI API error"
**Solution**: Check API key in `C:\Customer360\config\.env`

### Issue: "Module not found"
**Solution**: Ensure virtual environment is activated:
```powershell
C:\Customer360\venv\Scripts\Activate.ps1
pip install -r C:\Customer360\app\requirements.txt
```

### Issue: "ChromaDB not found"
**Solution**: Verify ChromaDB exists:
```powershell
Test-Path "C:\Customer360\VIBO\chromadb"
```

---

## Rollback (If Needed)

```powershell
# Stop all services
Disable-ScheduledTask -TaskName "Customer360-*"
Stop-ScheduledTask -TaskName "Customer360-VIBO-API"

# Remove installation
Remove-Item "C:\Customer360" -Recurse -Force

# Restore from backup if needed
```

---

## Support

For issues or questions:
- Check logs in `C:\Customer360\logs\`
- Run status check: `C:\Customer360\scripts\check_pipeline_status.ps1`
- Refer to: `C:\Customer360\app\DEPLOYMENT_GUIDE.md`
