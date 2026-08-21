#!/bin/bash

if [ -z "$3" ]; then
  echo "Usage: ./start-server.sh <nodeId> <rmiPort> <expectedClusterSize>"
  echo "Example: ./start-server.sh node-A 1099 3"
  exit 1
fi

echo "Starting Server Node $1 on Port $2 with Expected Cluster Size $3..."

mvn exec:java -Dexec.mainClass="Main" -Dexec.args="$1 $2 $3"