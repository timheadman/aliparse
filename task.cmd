@echo off
cd /d %~dp0
"venv\Scripts\python.exe" "main.py"
TIMEOUT /T 30