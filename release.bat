@echo off
set RELEASE=release\fdb2xml

if not exist dist\fdb2xml.exe (
    echo ERROR: dist\fdb2xml.exe not found. Run build.bat first.
    pause
    exit /b 1
)

echo Creating release...

if exist release rmdir /s /q release
mkdir %RELEASE%\runtime

copy dist\fdb2xml.exe %RELEASE%\
copy runtime\fbembed.dll %RELEASE%\runtime\
copy runtime\firebird.msg %RELEASE%\runtime\
copy runtime\ib_util.dll %RELEASE%\runtime\
copy runtime\icudt30.dll %RELEASE%\runtime\
copy runtime\icuin30.dll %RELEASE%\runtime\
copy runtime\icuuc30.dll %RELEASE%\runtime\
copy runtime\msvcp80.dll %RELEASE%\runtime\
copy runtime\msvcr80.dll %RELEASE%\runtime\
copy runtime\Microsoft.VC80.CRT.manifest %RELEASE%\runtime\

echo.
echo Release ready: %RELEASE%\
dir /b %RELEASE%
echo.
dir /b %RELEASE%\runtime
