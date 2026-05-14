import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class UDPMulticastSender extends Thread {

    private final String multicastGroup;
    private final int multicastPort;
    private final NodeInfo info;
    private boolean running = true;


    public UDPMulticastSender(String multicastGroup, int multicastPort, NodeInfo info) {
        this.multicastGroup = multicastGroup;
        this.multicastPort = multicastPort;
        this.info = info;
    }

    public void run() {
        try (MulticastSocket socket = new MulticastSocket()) {
            InetAddress group = InetAddress.getByName(multicastGroup);

            String payload = info.getNodeId() + "," + info.getIpAddress() + "," + info.getPort();
            byte[] payloadBytes = payload.getBytes();

            DatagramPacket packet = new DatagramPacket(payloadBytes, payloadBytes.length, group, info.getPort());

            while(running) {
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

    public String getMulticastGroup() {
        return multicastGroup;
    }

    public int getMulticastPort() {
        return multicastPort;
    }

    public NodeInfo getInfo() {
        return info;
    }
}
