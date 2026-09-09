import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Client {

    // Helper class to store discovered node coordinates
    private static class NodeCoordinates {
        final String ip;
        final int port;

        NodeCoordinates(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }
    }

    // Method to dynamically discover a live node using UDP Multicast
    private static NodeCoordinates discoverLiveNode() throws Exception {
        System.out.println("Listening for cluster heartbeats on 230.0.0.0:4446...");

        try (MulticastSocket socket = new MulticastSocket(4446)) {
            InetAddress group = InetAddress.getByName("230.0.0.0");
            socket.joinGroup(group);

            // Set a timeout to avoid infinite blocking if the cluster is completely down
            socket.setSoTimeout(10000);

            byte[] buffer = new byte[256];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

            // Block until the first heartbeat is received from any node
            socket.receive(packet);

            // Parse the payload: senderId, ip, port, activeJobs, term, isLeader
            String payload = new String(packet.getData(), 0, packet.getLength());
            String[] parts = payload.split(",");

            String targetIp = parts[1];
            int targetPort = Integer.parseInt(parts[2]);

            socket.leaveGroup(group);

            return new NodeCoordinates(targetIp, targetPort);
        }
    }

    public static void main(String[] args) {
        try {
            // 1. Discover a random active node dynamically
            NodeCoordinates targetNode = discoverLiveNode();
            System.out.println("Discovered and connecting to Executor at " + targetNode.ip + ":" + targetNode.port);

            // 2. Locate the RMI registry of the discovered executor
            Registry registry = LocateRegistry.getRegistry(targetNode.ip, targetNode.port);
            RemoteExecutorInterface executor = (RemoteExecutorInterface) registry.lookup("Executor");

            // 3. Create a generic Job
            Job<Integer> computeJob = new Job<>(() -> {
                System.out.println("[Job Execution] Calculating sum of 1 to 1000...");
                int sum = 0;
                for (int i = 1; i <= 1000; i++) {
                    sum += i;
                    try {
                        Thread.sleep(2); // Simulate time-consuming work
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                return sum;
            });

            // 4. Submit the job
            String returnedId = executor.submitJob(computeJob);
            System.out.println("Submitted Job ID: " + returnedId);

            // 5. Poll for result on the SAME node
            Object result = null;
            while (result == null) {
                result = executor.getJobResult(returnedId);
                if (result == null) {
                    System.out.println("Waiting for job to finish...");
                    Thread.sleep(1000);
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