# Distributed Job Scheduling System

A highly resilient, decentralized job scheduling system built with Java RMI and UDP Multicast. The system features dynamic load balancing, a Raft-style leader election (with Quorums and Terms), and Write-Ahead Logs (WAL) for crash recovery.

## Components

1. **Server (Executor Node):** The worker nodes that form the cluster. They use UDP multicast discovery to track each other, elect a Leader, and execute jobs in a local thread pool. They use a Write-Ahead Log (WAL) to survive crashes, and the leader dynamically distributes incoming work to the least-loaded node (including itself).
2. **Client:** Submits generic jobs (using Java Serialization and Code Mobility) and polls for results. It automatically discovers an active node via UDP multicast.

## Configuration & Network Setup

Since this system can run across distributed environments (such as mixed Linux and Windows machines), each server node explicitly defines its network interface address inside its entry point (`Main.java`). 

Before compiling and running the servers, make sure to set the `realIp` variable in `Main.java` to match the local network IP of the machine hosting the node:

```java
String realIp = "YOUR_NODE_IP_ADDRESS"; // e.g., "192.168.188.31"

```

## How to Run

To make execution easy, both Windows Batch (`.bat`) and Linux Shell (`.sh`) scripts are provided.

### 1. Start the Cluster (Servers)

Open a terminal for each node you want to start. You must provide a unique Node ID, an RMI Port, and the Expected Cluster Size (to enforce strict Quorums and prevent Split-Brain).

**On Linux:**

```bash
./start-server.sh node-A 1099 3
./start-server.sh node-B 1100 3
./start-server.sh node-C 1101 3

```

**On Windows:**

```powershell
.\start-server.bat node-A 1099 3
.\start-server.bat node-B 1100 3
.\start-server.bat node-C 1101 3

```

*Wait ~10 seconds for the nodes to time out, hold an election, and elect a Leader!*

### 2. Start the Client

Open a new terminal and start the Client. It will listen to the network via UDP multicast to discover a live node, or it can accept a targeted IP address as an optional parameter.

**On Linux:**

```bash
./start-client.sh

```

**On Windows:**

```powershell
.\start-client.bat

```

The client will automatically submit a generic job, poll for execution status, and print the final result once processing is complete.

## Troubleshooting & Compilation Notes

If you encounter any issues regarding missing dependencies, outdated class files, or network changes failing to take effect after modifying the source code, always make sure to clean and recompile the project before launching the scripts. 
Run the following command in your project root directory:

```bash
mvn clean compile
```
This ensures that all Java source files are freshly compiled and that your latest code changes and configurations are properly applied.