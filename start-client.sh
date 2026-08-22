#!/bin/bash

# Check if the second argument is missing
#if [ -z "$2" ]; then
#    echo "Usage: ./start-client.sh <gatewayIp> <gatewayPort>"
#    echo "Example: ./start-client.sh 192.168.1.5 8000"
#    exit 1
#fi

# Display startup message and launch the client via Maven
echo "Starting Client and listening the network..."
mvn exec:java -Dexec.mainClass="Client"