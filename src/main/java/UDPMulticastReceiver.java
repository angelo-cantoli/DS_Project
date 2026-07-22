import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class UDPMulticastReceiver extends Thread {
    private final String multicastGroup;
    private final int multicastPort;
    private final ClusterManager clusterManager;
    private final String nodeId;
    private boolean running = true;

    public UDPMulticastReceiver(String multicastGroup, int multicastPort, ClusterManager clusterManager, String nodeId) {
        this.multicastGroup = multicastGroup;
        this.multicastPort = multicastPort;
        this.clusterManager = clusterManager;
        this.nodeId = nodeId;
    }

    @Override
    public void run() {
        try (MulticastSocket socket = new MulticastSocket(multicastPort)) {
            // Removed 127.0.0.1 hardcode to allow binding to physical Wi-Fi/Ethernet cards
            InetAddress group = InetAddress.getByName(multicastGroup);
            socket.joinGroup(group);

            byte[] buffer = new byte[256];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

            while(running){
                socket.receive(packet);
                String receivedData = new String(packet.getData(),  0, packet.getLength());

                String[] parts = receivedData.split(",");
                if(parts.length >= 3){
                    String senderId = parts[0];
                    String senderIp = parts[1];
                    int senderPort = Integer.parseInt(parts[2]);
                    int activeJobs = parts.length > 3 ? Integer.parseInt(parts[3]) : 0;
                    int term = parts.length > 4 ? Integer.parseInt(parts[4]) : 0;
                    boolean isLeader = parts.length > 5 ? Boolean.parseBoolean(parts[5]) : false;

                    if(!senderId.equals(nodeId)){
                        NodeInfo info = new NodeInfo(senderId, senderIp, senderPort, activeJobs, term, isLeader);
                        clusterManager.isAlive(info);
                    }
                }
            }

            socket.leaveGroup(group);
        } catch (IOException e) {
            if (running) {
                System.out.println("Problema nella ricezione dell'heartbeat: " + e.getMessage());
            }
        }
    }

    public void stopReceiver(){
        running = false;
        this.interrupt();
    }
}
