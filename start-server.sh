#!/bin/bash

if [ -z "$2" ]; then
  echo "Usage: ./start-server.sh <nodeId> <expectedClusterSize>"
  echo "Example: ./start-server.sh node-A 3"
  exit 1
fi

echo "Starting Server Node $1 with Expected Cluster Size $2 (Port is auto-assigned)..."

mvn exec:java -Dexec.mainClass="Main" -Dexec.args="$1 $2"