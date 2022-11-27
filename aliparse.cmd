@echo off
cd /d %~dp0
"venv\Scripts\python.exe" "main.py"
rem TIMEOUT /T 30