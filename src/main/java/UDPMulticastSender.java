import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class UDPMulticastSender extends Thread {
    private final String multicastGroup;
    private final int multicastPort;
    private final NodeInfo info;
    private final Executor executor;
    private final ClusterManager clusterManager;
    private boolean running = true;

    public UDPMulticastSender(String multicastGroup, int multicastPort, NodeInfo info, Executor executor, ClusterManager clusterManager) {
        this.multicastGroup = multicastGroup;
        this.multicastPort = multicastPort;
        this.info = info;
        this.executor = executor;
        this.clusterManager = clusterManager;
    }

    public void run() {
        try (MulticastSocket socket = new MulticastSocket()) {
            InetAddress group = InetAddress.getByName(multicastGroup);

            while(running) {
                int activeJobs = executor != null ? executor.getActiveJobsCount() : 0;
                int term = clusterManager != null ? clusterManager.getCurrentTerm() : 0;
                boolean isLeader = clusterManager != null && clusterManager.isLeader();
                //DEBUG
                if (activeJobs > 0) {
                    System.out.println("[UDP SENDER] My real load is: " + activeJobs + ". Sending to cluster...");
                }

                String payload = info.getNodeId() + "," + info.getIpAddress() + "," + info.getPort() + "," + activeJobs + "," + term + "," + isLeader;
                byte[] payloadBytes = payload.getBytes();

                DatagramPacket packet = new DatagramPacket(payloadBytes, payloadBytes.length, group, multicastPort);
                socket.send(packet);
                Thread.sleep(3000);
            }
        } catch (IOException | InterruptedException e) {
            if (running) {
                System.out.println("Problema nel sender di Hearthbeat: " + e.getMessage());
            }
        }
    }

    public void stopSender() {
        running = false;
        this.interrupt();
    }
}
