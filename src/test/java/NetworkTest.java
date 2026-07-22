import java.util.Random;

public class NetworkTest {

    public static void main(String[] args) throws Exception {
        // 1. Generiamo dati fittizi per simulare un nodo
        Random rand = new Random();
        String myNodeId = "NodeTest-" + rand.nextInt(1000);
        String myIp = java.net.InetAddress.getLocalHost().getHostAddress();
        int myRmiPort = 1000 + rand.nextInt(9000); 

        // New NodeInfo Signature: nodeId, ip, port, activeJobs, term, isLeader
        NodeInfo myInfo = new NodeInfo(myNodeId, myIp, myRmiPort, 0, 0, false);
        System.out.println("=== AVVIO NODO DI TEST: " + myNodeId + " ===");

        // Parametri Multicast (uguali a Main.java)
        String multicastGroup = "230.0.0.0";
        int multicastPort = 4446;
        int expectedClusterSize = 3; // Necessario per il Quorum

        // 2. Inizializziamo i componenti aggiornati
        ClusterManager clusterManager = new ClusterManager(myNodeId, expectedClusterSize);
        clusterManager.isAlive(myInfo); // Aggiungiamo noi stessi per i calcoli del Quorum

        UDPMulticastReceiver receiver = new UDPMulticastReceiver(
                multicastGroup, multicastPort, clusterManager, myNodeId
        );

        // UDPMulticastSender riceve null come Executor per questo test puro di rete
        UDPMulticastSender sender = new UDPMulticastSender(
                multicastGroup, multicastPort, myInfo, null, clusterManager
        );

        // 3. Avviamo i thread
        receiver.start();
        sender.start();

        // 4. Loop di monitoraggio
        while (true) {
            Thread.sleep(3000);
            System.out.println("\n[" + myNodeId + "] Nodi attualmente vivi (" + clusterManager.getAliveNodes().size() + "):");
            for (NodeInfo node : clusterManager.getAliveNodes()) {
                System.out.println("   -> ID: " + node.getNodeId() + ", IP: " + node.getIpAddress() + 
                                   ", Port: " + node.getPort() + ", Jobs: " + node.getActiveJobs() + 
                                   ", Term: " + node.getTerm() + ", isLeader: " + node.isLeader());
            }
            System.out.println("--- [Raft Status] Leader Riconosciuto: " + clusterManager.getCurrentLeader() + 
                               " | Term: " + clusterManager.getCurrentTerm() + " | Sono Leader? " + clusterManager.isLeader());
        }
    }
}