@echo off
if "%~2"=="" (
    echo Usage: start-client.bat ^<gatewayIp^> ^<gatewayPort^>
    echo Example: start-client.bat 192.168.1.5 8000
    exit /b 1
)
echo Starting Client and connecting to Gateway at %1:%2...
mvn exec:java "-Dexec.mainClass=Client" "-Dexec.args=%1 %2"
