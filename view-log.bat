@echo off
if "%~1"=="" (
    echo Usage: view-log.bat ^<nodeId^>
    echo Example: view-log.bat node-A
    exit /b 1
)

mvn exec:java -Dexec.mainClass="LogViewer" -Dexec.args="%~1"
