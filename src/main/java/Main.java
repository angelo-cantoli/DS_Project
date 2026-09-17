import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: java Main <nodeId> <rmiPort> <expectedClusterSize>");
            System.exit(1);
        }

        String nodeId = args[0];
        int rmiPort = Integer.parseInt(args[1]);
        int expectedClusterSize = Integer.parseInt(args[2]);
        //la "stanza" che abbiamo impostato per il multicast 230.0.0.0:4446
        String multicastGroup = "230.0.0.0";
        int multicastPort = 4446;

        //----------------------------------------------
        //------ DOBBIAMO METTERCELO A MANO!!! -------
        //----------------------------------------------
        String realIp = "172.20.10.3";
        System.setProperty("java.rmi.server.hostname", realIp);
        System.out.println("[INFO] Node " + nodeId + " is binding to IP: " + realIp + " on port " + rmiPort);


        try {
            ClusterManager clusterManager = new ClusterManager(nodeId, expectedClusterSize);

            Executor executor = new Executor(nodeId, clusterManager, rmiPort);
            Registry registry = LocateRegistry.createRegistry(rmiPort);
            registry.rebind("Executor", executor);

            NodeInfo selfInfo = new NodeInfo(nodeId, realIp, rmiPort, 0, 0, false);
            clusterManager.isAlive(selfInfo);

            //Start Multicast discovery
            UDPMulticastReceiver receiver = new UDPMulticastReceiver( multicastPort, clusterManager, nodeId);
            receiver.start();

            List<String> clusterIps = Arrays.asList("172.20.10.2", "172.20.10.3", "172.20.10.4");
            UDPMulticastSender sender = new UDPMulticastSender( clusterIps, multicastPort, selfInfo, executor, clusterManager);
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
