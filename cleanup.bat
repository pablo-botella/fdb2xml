@echo off
echo Cleaning...
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
if exist release rmdir /s /q release
##### if exist runtime rmdir /s /q runtime
if exist __pycache__ rmdir /s /q __pycache__
if exist fdb2xml.spec del fdb2xml.spec
echo Done.
