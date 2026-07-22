@echo off
if "%~3"=="" (
    echo Usage: start-server.bat ^<nodeId^> ^<rmiPort^> ^<expectedClusterSize^>
    echo Example: start-server.bat node-A 1099 3
    exit /b 1
)
echo Starting Server Node %1 on Port %2 with Expected Cluster Size %3...
mvn exec:java "-Dexec.mainClass=Main" "-Dexec.args=%1 %2 %3"
