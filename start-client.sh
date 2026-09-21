#!/bin/bash

# Display startup message and launch the client via Maven
echo "Starting ClientTest and listening the network..."
mvn exec:java -Dexec.mainClass="ClientTest"