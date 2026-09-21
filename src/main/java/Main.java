import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java Main <nodeId> [<rmiPort>] <expectedClusterSize>");
            System.exit(1);
        }

        String nodeId = args[0];
        int expectedClusterSize = Integer.parseInt(args[args.length - 1]);
        
        String realIp = Config.getMyIp();

        System.setProperty("java.rmi.server.hostname", realIp);

        int rmiPort = 1099;
        int multicastPort = 4446;
        Registry registry = null;
        UDPMulticastReceiver receiver = null;

        ClusterManager clusterManager = new ClusterManager(nodeId, expectedClusterSize);
        Executor executor = null;

        for (int i = 0; i <= 6; i++) { // Range 1099 to 1105
            rmiPort = 1099 + i;
            multicastPort = 4446 + i;
            try {
                registry = LocateRegistry.createRegistry(rmiPort);
                executor = new Executor(nodeId, clusterManager, rmiPort);
                registry.rebind("Executor", executor);
                
                receiver = new UDPMulticastReceiver(multicastPort, clusterManager, nodeId);
                receiver.start(); // If execption port is occupied
                
                System.out.println("[INFO] Node " + nodeId + " SUCCESSFULLY bound to IP: " + realIp + " | RMI Port: " + rmiPort + " | UDP Port: " + multicastPort);
                break;
            } catch (Exception e) {
                if (registry != null) {
                    try { java.rmi.server.UnicastRemoteObject.unexportObject(registry, true); } catch (Exception ignored) {}
                }
                if (receiver != null) {
                    receiver.stopReceiver();
                }
                registry = null;
                System.out.println("[INFO] Port " + rmiPort + " (or UDP " + multicastPort + ") in use. Trying next...");
            }
        }

        if (registry == null || executor == null) {
            System.err.println("[FATAL] Could not find any free ports in the range 1099-1105.");
            System.exit(1);
        }

        try {
            NodeInfo selfInfo = new NodeInfo(nodeId, realIp, rmiPort, 0, 0, false);
            clusterManager.isAlive(selfInfo);

            List<String> clusterIps = Config.getClusterIps();
            UDPMulticastSender sender = new UDPMulticastSender(clusterIps, selfInfo, executor, clusterManager);
            sender.start();

            System.out.println("=== Node " + nodeId + " is running on port " + rmiPort + " ===");
            
            // Background thread to log Raft status
            new Thread(() -> {
                try {
                    while (true) {
                        Thread.sleep(5000);
                        System.out.println("[Raft Status] Leader: " + clusterManager.getCurrentLeader() + 
                                         " | Term: " + clusterManager.getCurrentTerm() +
                                         " | LastLogIndex: " + clusterManager.getRaftLog().getLastLogIndex() +
                                         " | LastLogTerm: " + clusterManager.getRaftLog().getLastLogTerm() +
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
