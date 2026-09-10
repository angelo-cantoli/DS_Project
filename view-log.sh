#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: ./view-log.sh <nodeId>"
  echo "Example: ./view-log.sh node-A"
  echo "Example: ./view-log.sh node-B"
  exit 1
fi

mvn exec:java -Dexec.mainClass="LogViewer" -Dexec.args="$1"
