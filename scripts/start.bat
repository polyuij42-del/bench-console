@echo off
REM Bench Console - one-click launcher (Windows)
REM Usage: scripts\start.bat   (or double-click)
setlocal
cd /d "%~dp0.."

where node >nul 2>nul
if errorlevel 1 (
  echo [x] Node.js not found. Install Node 18+ first: https://nodejs.org/
  pause
  exit /b 1
)

for /f "tokens=1 delims=." %%v in ('node -p "process.versions.node"') do set NODEMAJOR=%%v
if %NODEMAJOR% LSS 18 (
  echo [x] Node 18+ required, found:
  node -v
  pause
  exit /b 1
)

if not exist config.json (
  echo - copying config.example.json to config.json
  copy /y config.example.json config.json >nul
  echo - edit config.json services to match your setup, then run again.
  pause
)

echo - starting Bench Console...
node bench-console.js
pause
