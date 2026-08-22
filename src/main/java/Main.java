import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Main {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: java Main <nodeId> <rmiPort> <expectedClusterSize>");
            System.exit(1);
        }

        String nodeId = args[0];
        int rmiPort = Integer.parseInt(args[1]);
        int expectedClusterSize = Integer.parseInt(args[2]);
        String multicastGroup = "230.0.0.0";
        int multicastPort = 4446;

        //----------------------------------------------
        //------ IMPOSTARE A MANO TALE PARAMETRO -------
        //----------------------------------------------
        String realIp = "192.168.178.40";
        System.setProperty("java.rmi.server.hostname", realIp);
        System.out.println("[INFO] Node " + nodeId + " is binding to IP: " + realIp + " on port " + rmiPort);


        try {
            // 1. Initialize Cluster Manager with Raft logic
            ClusterManager clusterManager = new ClusterManager(nodeId, expectedClusterSize);

            // 2. Setup RMI Executor
            Executor executor = new Executor(nodeId, clusterManager);

            // Create RMI Registry locally
            Registry registry = LocateRegistry.createRegistry(rmiPort);
            registry.rebind("Executor", executor);

            // Manually add self
            NodeInfo selfInfo = new NodeInfo(nodeId, realIp, rmiPort, 0, 0, false);
            clusterManager.isAlive(selfInfo);

            // 3. Start Multicast discovery
            UDPMulticastReceiver receiver = new UDPMulticastReceiver(multicastGroup, multicastPort, clusterManager, nodeId);
            receiver.start();
            
            // Sender needs reference to ClusterManager to broadcast current Term
            UDPMulticastSender sender = new UDPMulticastSender(multicastGroup, multicastPort, selfInfo, executor, clusterManager);
            sender.start();

            System.out.println("=== Node " + nodeId + " is running on port " + rmiPort + " ===");
            
            // Background thread to log Raft status
            new Thread(() -> {
                try {
                    while (true) {
                        Thread.sleep(5000);
                        System.out.println("[Raft Status] Leader: " + clusterManager.getCurrentLeader() + 
                                         " | Term: " + clusterManager.getCurrentTerm() +
                                         " | Am I Leader? " + clusterManager.isLeader());
                    }
                } catch (InterruptedException e) { }
            }).start();

        } catch (Exception e) {
            System.err.println("Failed to start Executor Node:");
            e.printStackTrace();
        }
    }
}
