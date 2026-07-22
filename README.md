# Distributed Job Scheduling System

A highly resilient, decentralized job scheduling system built with Java RMI and UDP Multicast. The system features dynamic load balancing, a Raft-style leader election (with Quorums and Terms), and Write-Ahead Logs (WAL) for crash recovery.

## Components

1. **Server (Executor Node):** The worker nodes that form the cluster. They use UDP gossip to discover each other, elect a Leader, and execute jobs in a local thread pool. They use a Write-Ahead Log (WAL) to survive power outages.
2. **API Gateway (Load Balancer):** A seamless edge proxy. It passively listens to the cluster's UDP heartbeats to track the active Leader. It hides all cluster complexity and network failures from the Client.
3. **Client:** Submits generic jobs (using Java Serialization and Code Mobility) and polls for results. It is completely "dumb" and only connects to the Gateway.

## How to Run

To make execution easy, convenient Windows Batch (`.bat`) scripts have been provided. **Note:** All components must be running on the same Local Area Network (LAN/Wi-Fi) for UDP Multicast discovery to work.

### 1. Start the Cluster (Servers)
Open a new terminal for each node you want to start. You must provide a unique Node ID, an RMI Port, and the Expected Cluster Size (to enforce strict Quorums and prevent Split-Brain).

```powershell
.\start-server.bat node-A 1099 3
.\start-server.bat node-B 1100 3
.\start-server.bat node-C 1101 3
```
*Wait ~10 seconds for the nodes to timeout, hold an election, and elect a Leader!*

### 2. Start the API Gateway
Open a new terminal and start the Gateway. It automatically runs on port `8000`.

```powershell
.\start-gateway.bat
```
*The Gateway will print out its real LAN IP Address. Copy this IP!*

### 3. Start the Client
Open a new terminal and start the Client, passing the Gateway's IP address and Port (`8000`).

```powershell
.\start-client.bat <GATEWAY_IP_ADDRESS> 8000
```
*Example: `.\start-client.bat 192.168.1.5 8000`*

The client will automatically submit a job, and the Gateway will dynamically route it to the active Leader for load-balanced execution!
