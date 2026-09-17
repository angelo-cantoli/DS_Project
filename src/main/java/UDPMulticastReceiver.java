import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

public class UDPMulticastReceiver extends Thread {
    private final int port;
    private final ClusterManager clusterManager;
    private final String nodeId;
    private boolean running = true;

    // RImosso il gruppo
    public UDPMulticastReceiver(int port, ClusterManager clusterManager, String nodeId) {
        this.port = port;
        this.clusterManager = clusterManager;
        this.nodeId = nodeId;
    }

    @Override
    public void run() {
        // Sostituito MulticastSocket con DatagramSocket
        try (DatagramSocket socket = new DatagramSocket(port)) {

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
                    //se ha dei lavori attivi allora prendi quanti sono altrimenti 0
                    int activeJobs = parts.length > 3 ? Integer.parseInt(parts[3]) : 0;
                    //se ha specificato in che term pensa di essere salvalo
                    int term = parts.length > 4 ? Integer.parseInt(parts[4]) : 0;
                    //se fosse leader ricordatelo
                    boolean isLeader = parts.length > 5 ? Boolean.parseBoolean(parts[5]) : false;

                    if(!senderId.equals(nodeId)){
                        NodeInfo info = new NodeInfo(senderId, senderIp, senderPort, activeJobs, term, isLeader);
                        clusterManager.isAlive(info);
                    }
                }
            }
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