@echo off
if "%~2"=="" (
    echo Usage: start-server.bat ^<nodeId^> ^<expectedClusterSize^>
    echo Example: start-server.bat node-A 3
    exit /b 1
)
echo Starting Server Node %1 with Expected Cluster Size %2 (Port is auto-assigned)...
mvn exec:java "-Dexec.mainClass=Main" "-Dexec.args=%1 %2"
