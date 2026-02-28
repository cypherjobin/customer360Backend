# VS Code + Claude Code Setup on Production Server

## Quick Setup

### 1. Install VS Code

```powershell
# Download and install VS Code (User mode - no admin needed)
$ProgressPreference = 'SilentlyContinue'
Invoke-WebRequest -Uri "https://code.visualstudio.com/sha/download?build=stable&os=win32-x64-user" -OutFile "$env:TEMP\VSCodeUserSetup.exe"
Start-Process -FilePath "$env:TEMP\VSCodeUserSetup.exe" -ArgumentList "/verysilent /mergetasks=!runcode" -Wait

# Or download system installer (requires admin)
# Invoke-WebRequest -Uri "https://code.visualstudio.com/sha/download?build=stable&os=win32-x64" -OutFile "$env:TEMP\VSCodeSetup.exe"
# Start-Process -FilePath "$env:TEMP\VSCodeSetup.exe" -ArgumentList "/verysilent /mergetasks=!runcode" -Wait
```

### 2. Install Claude Code Extension

```powershell
# Method 1: Via command line
code --install-extension Claude.Claude-C

# Method 2: Open VS Code and install from marketplace
# 1. Open VS Code
# 2. Press Ctrl+Shift+X
# 3. Search for "Claude Code"
# 4. Click Install
```

### 3. Open Customer 360 Project

```powershell
# Open project in VS Code
code C:\Customer360
```

---

## Recommended Extensions

```powershell
# Python
code --install-extension ms-python.python

# PowerShell
code --install-extension ms-vscode.PowerShell

# Pylance (Python IntelliSense)
code --install-extension ms-python.vscode-pylance

# Jupyter (if needed)
code --install-extension ms-toolsai.jupyter

# GitLens (Git supercharged)
code --install-extension eamodio.gitlens
```

---

## VS Code Settings for Customer 360

Create `.vscode/settings.json` in `C:\Customer360\`:

```json
{
    "python.defaultInterpreterPath": "C:\\Customer360\\venv\\Scripts\\python.exe",
    "python.terminal.activateEnvironment": true,
    "python.linting.enabled": true,
    "python.linting.pylintEnabled": true,
    "python.formatting.provider": "black",
    "files.autoSave": "afterDelay",
    "files.autoSaveDelay": 1000,
    "editor.formatOnSave": true,
    "editor.fontSize": 14,
    "terminal.integrated.fontSize": 13,
    "workbench.colorTheme": "Default Dark+",
    "python.analysis.extraPaths": [
        "C:\\Customer360",
        "C:\\Customer360\\VIBO"
    ]
}
```

---

## Useful VS Code Tasks

Create `.vscode/tasks.json` in `C:\Customer360\`:

```json
{
    "version": "2.0.0",
    "tasks": [
        {
            "label": "Run Daily Pipeline",
            "type": "shell",
            "command": "venv\\Scripts\\python.exe",
            "args": ["app\\run_daily_pipeline.py"],
            "options": {
                "cwd": "C:\\Customer360"
            },
            "group": {
                "kind": "build",
                "isDefault": true
            },
            "problemMatcher": []
        },
        {
            "label": "Start VIBO API",
            "type": "shell",
            "command": "venv\\Scripts\\python.exe",
            "args": ["VIBO\\vibo_api.py"],
            "options": {
                "cwd": "C:\\Customer360"
            },
            "problemMatcher": []
        },
        {
            "label": "Check Pipeline Status",
            "type": "shell",
            "command": "powershell",
            "args": ["-File", "scripts\\check_pipeline_status.ps1"],
            "options": {
                "cwd": "C:\\Customer360"
            },
            "problemMatcher": []
        },
        {
            "label": "Run LLM Summarizer",
            "type": "shell",
            "command": "venv\\Scripts\\python.exe",
            "args": ["app\\llm_summariser_v4.py"],
            "options": {
                "cwd": "C:\\Customer360"
            },
            "problemMatcher": []
        },
        {
            "label": "Update VIBO Embeddings",
            "type": "shell",
            "command": "venv\\Scripts\\python.exe",
            "args": ["VIBO\\vibo_embedding_pipeline.py"],
            "options": {
                "cwd": "C:\\Customer360"
            },
            "problemMatcher": []
        }
    ]
}
```

---

## Run Tasks from VS Code

1. Press `Ctrl+Shift+B` to run the default task (Daily Pipeline)
2. Press `Ctrl+Shift+P` → "Tasks: Run Task" → Select task

---

## Claude Code Tips

### Starting a New Session

1. Open VS Code
2. Press `Ctrl+Shift+A` to open Claude Code
3. Start coding!

### Useful Claude Code Commands

| Command | Description |
|---------|-------------|
| `/help` | Show all commands |
| `/clear` | Clear conversation |
| `/save` | Save conversation to file |
| `/load` | Load conversation from file |

### Project Context

Claude Code will automatically see:
- All files in `C:\Customer360\`
- Your `.env` configuration
- Database connections
- Everything needed to continue development

---

## Quick Start Checklist

- [ ] Install VS Code
- [ ] Install Claude Code extension
- [ ] Open `C:\Customer360` in VS Code
- [ ] Run `Terminal: Create New Terminal` (Ctrl+`)
- [ ] Verify Python path: `Get-Command python`
- [ ] Activate venv: `C:\Customer360\venv\Scripts\Activate.ps1`
- [ ] Test: `python -c "import pyodbc, chromadb, fastapi"`
- [ ] Open Claude Code: `Ctrl+Shift+A`

---

## Troubleshooting

### VS Code won't install extensions
**Solution**: Check internet connection and try again

### Python not found in VS Code
**Solution**:
1. Press `Ctrl+Shift+P`
2. Type "Python: Select Interpreter"
3. Choose `C:\Customer360\venv\Scripts\python.exe`

### Can't activate virtual environment
**Solution**:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Claude Code not connecting
**Solution**: Check your Claude API key in settings

---

## Sync with Development Machine

To copy changes back to development machine:

```powershell
# From production server, copy to shared drive
Copy-Item "C:\Customer360\app\*.py" -Destination "Y:\Customer360-Backup\" -Force

# On development machine, copy back
Copy-Item "Y:\Customer360-Backup\*.py" -Destination "C:\Projects\Customer360\" -Force
```

---

## Tips for Working on Production Server

1. **Use Integrated Terminal**: `Ctrl+`` in VS Code
2. **Auto-save**: Enabled by default (1 second delay)
3. **Git integration**: Use GitLens for better git experience
4. **Split view**: `Ctrl+\` to split editor
5. **Command Palette**: `Ctrl+Shift+P` for all commands
