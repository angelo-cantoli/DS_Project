import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Client {

    private static class NodeCoordinates {
        final String ip;
        final int port;

        NodeCoordinates(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }
    }

    private static NodeCoordinates discoverLiveNode() throws Exception {
        System.out.println("Listening for cluster heartbeats on 230.0.0.0:4446...");
        try (MulticastSocket socket = new MulticastSocket(4446)) {
            InetAddress group = InetAddress.getByName("230.0.0.0");
            socket.joinGroup(group);
            socket.setSoTimeout(10000);
            byte[] buffer = new byte[256];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            socket.receive(packet);
            String payload = new String(packet.getData(), 0, packet.getLength());
            String[] parts = payload.split(",");
            String targetIp = parts[1];
            int targetPort = Integer.parseInt(parts[2]);
            socket.leaveGroup(group);
            return new NodeCoordinates(targetIp, targetPort);
        }
    }

    private static RemoteExecutorInterface getExecutorStub() throws Exception {
        NodeCoordinates targetNode = discoverLiveNode();
        System.out.println("Discovered and connecting to Executor at " + targetNode.ip + ":" + targetNode.port);
        Registry registry = LocateRegistry.getRegistry(targetNode.ip, targetNode.port);
        return (RemoteExecutorInterface) registry.lookup("Executor");
    }

    public static void main(String[] args) {
        try {
            RemoteExecutorInterface executor = getExecutorStub();

            Job<Integer> computeJob = new Job<>(() -> {
                System.out.println("[Job Execution] Calculating sum of 1 to 1000...");
                int sum = 0;
                for (int i = 1; i <= 1000; i++) {
                    sum += i;
                    try { Thread.sleep(2); } catch (InterruptedException e) { e.printStackTrace(); }
                }
                return sum;
            });

            String returnedId = null;
            while (returnedId == null) {
                try {
                    returnedId = executor.submitJob(computeJob);
                } catch (RemoteException e) {
                    System.err.println("Connection lost while submitting! Reconnecting to a new node...");
                    executor = getExecutorStub();
                }
            }
            System.out.println("Submitted Job ID: " + returnedId);

            Object result = null;
            while (result == null) {
                try {
                    result = executor.getJobResult(returnedId);
                    if (result == null) {
                        System.out.println("Waiting for job to finish...");
                        Thread.sleep(1000);
                    }
                } catch (RemoteException e) {
                    System.err.println("Connection lost while polling! Reconnecting to a new node...");
                    executor = getExecutorStub();
                }
            }

            System.out.println("Job Result: " + result);

        } catch (java.net.SocketTimeoutException e) {
            System.err.println("Timeout: No active nodes discovered. Is the cluster running?");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}