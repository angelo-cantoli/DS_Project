import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.List;

public class UDPMulticastSender extends Thread {
    // multicastGroup sostituito con una lista di ip da passare nel momento in cui viene chiamato il costruttore (per remoto ip di Tailscale)
    private final List<String> peerIps;
    private final int port;
    private final NodeInfo info;
    private final Executor executor;
    private final ClusterManager clusterManager;
    private boolean running = true;

    public UDPUnicastSender(List<String> peerIps, int port, NodeInfo info, Executor executor, ClusterManager clusterManager) {
        this.peerIps = peerIps;
        this.port = port;
        this.info = info;
        this.executor = executor;
        this.clusterManager = clusterManager;
    }

    public void run() {
        // DatagramSocket invece di MulticastSocket
        try (DatagramSocket socket = new DatagramSocket()) {

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

                // Iteriamo su tutti gli ip
                for (String peerIp : peerIps) {
                    try {
                        InetAddress peerAddress = InetAddress.getByName(peerIp);
                        DatagramPacket packet = new DatagramPacket(payloadBytes, payloadBytes.length, peerAddress, port);
                        socket.send(packet);
                    } catch (IOException e) {
                        System.out.println("Impossibile inviare heartbeat al nodo " + peerIp + ": " + e.getMessage());
                    }
                }

                Thread.sleep(3000);
            }
        } catch (InterruptedException | IOException e) {
            if (running) {
                System.out.println("Problema nel sender di Heartbeat: " + e.getMessage());
            }
        }
    }

    public void stopSender() {
        running = false;
        this.interrupt();
    }
}