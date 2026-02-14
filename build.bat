@echo off
echo Compiling fdb2xml...
pyinstaller --onefile --name fdb2xml --clean fdb2xml.py
if %errorlevel% neq 0 (
    echo BUILD FAILED
    pause
    exit /b 1
)
echo Done: dist\fdb2xml.exe
