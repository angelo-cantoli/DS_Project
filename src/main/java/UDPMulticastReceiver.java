import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class UDPMulticastReceiver extends Thread{

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
            socket.setInterface(InetAddress.getByName("127.0.0.1"));
            InetAddress group = InetAddress.getByName(multicastGroup);
            socket.joinGroup(group);

            byte[] buffer = new byte[256];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

            while(running){
                socket.receive(packet);
                String receivedData = new String(packet.getData(),  0, packet.getLength());

                String[] parts = receivedData.split(",");
                if(parts.length == 3){
                    String senderId = parts[0];
                    String senderIp = parts[1];
                    int senderPort = Integer.parseInt(parts[2]);

                    //Costruisco il NodeInfo e mando heartbeat al clustemanager
                    if(!senderId.equals(nodeId)){
                        NodeInfo info = new NodeInfo(senderId, senderIp, senderPort);
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
