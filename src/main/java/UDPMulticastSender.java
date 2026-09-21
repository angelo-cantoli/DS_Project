import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.List;

public class UDPMulticastSender extends Thread {
    // multicastGroup sostituito con una lista di ip da passare nel momento in cui viene chiamato il costruttore (per remoto ip di Tailscale)
    private final List<String> peerIps;
    private final int basePort = 4446;
    private final NodeInfo info;
    private final Executor executor;
    private final ClusterManager clusterManager;
    private boolean running = true;

    public UDPMulticastSender(List<String> peerIps, NodeInfo info, Executor executor, ClusterManager clusterManager) {
        this.peerIps = peerIps;
        this.info = info;
        this.executor = executor;
        this.clusterManager = clusterManager;
    }

    public void run() {

        try (DatagramSocket socket = new DatagramSocket()) {

            while(running) {
                int activeJobs = executor != null ? executor.getActiveJobsCount() : 0;
                int term = clusterManager != null ? clusterManager.getCurrentTerm() : 0;
                boolean isLeader = clusterManager != null && clusterManager.isLeader();

                String payload = info.getNodeId() + "," + info.getIpAddress() + "," + info.getPort() + "," + activeJobs + "," + term + "," + isLeader;
                byte[] payloadBytes = payload.getBytes();

                //iterate on ip and ports of range
                for (String peerIp : peerIps) {
                    try {
                        InetAddress peerAddress = InetAddress.getByName(peerIp);
                        for (int p = basePort; p <= basePort + 6; p++) {
                            DatagramPacket packet = new DatagramPacket(payloadBytes, payloadBytes.length, peerAddress, p);
                            socket.send(packet);
                        }
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