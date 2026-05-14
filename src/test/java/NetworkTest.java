import java.util.Random;

public class NetworkTest {

    public static void main(String[] args) throws InterruptedException {
        // 1. Generiamo dati fittizi per simulare un nodo unico ad ogni avvio
        Random rand = new Random();
        String myNodeId = "Node-" + rand.nextInt(1000);
        String myIp = "127.0.0.1"; // Usiamo localhost per i test
        int myRmiPort = 1000 + rand.nextInt(9000); // Porta casuale fittizia

        NodeInfo myInfo = new NodeInfo(myNodeId, myIp, myRmiPort);
        System.out.println("=== AVVIO NODO DI TEST: " + myNodeId + " ===");

        // Parametri Multicast (devono essere uguali per tutti i nodi)
        String multicastGroup = "230.0.0.1";
        int multicastPort = 4446;

        // 2. Inizializziamo i componenti
        ClusterManager clusterManager = new ClusterManager();

        UDPMulticastReceiver receiver = new UDPMulticastReceiver(
                multicastGroup, multicastPort, clusterManager, myNodeId
        );

        UDPMulticastSender sender = new UDPMulticastSender(
                multicastGroup, multicastPort, myInfo
        );

        // 3. Avviamo i thread
        receiver.start();
        sender.start();

        // 4. Loop di monitoraggio: ogni 3 secondi stampiamo lo stato del cluster
        while (true) {
            Thread.sleep(3000);
            System.out.println("\n[" + myNodeId + "] Nodi attualmente vivi (" + clusterManager.getAliveNodes().size() + "):");
            for (NodeInfo node : clusterManager.getAliveNodes()) {
                System.out.println("   -> " + node.toString());
            }
        }
    }
}