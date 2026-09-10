import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.rmi.RemoteException;
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

    private static NodeCoordinates currentTarget = null;
    private static RemoteExecutorInterface currentExecutor = null;

    // Method to dynamically discover a live node using UDP Multicast with retry
    private static NodeCoordinates discoverLiveNode() {
        while (true) {
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
                System.out.println("Discovered active executor node at " + targetIp + ":" + targetPort);
                return new NodeCoordinates(targetIp, targetPort);
            } catch (Exception e) {
                System.err.println("Discovery timeout or error (" + e.getMessage() + "). Retrying discovery in 2 seconds...");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ignored) {}
            }
        }
    }

    private static RemoteExecutorInterface getConnectedExecutor() {
        while (currentExecutor == null) {
            try {
                // If we already had coordinates, try to reconnect first (in case node rebooted)
                if (currentTarget != null) {
                    try {
                        Registry reg = LocateRegistry.getRegistry(currentTarget.ip, currentTarget.port);
                        currentExecutor = (RemoteExecutorInterface) reg.lookup("Executor");
                        return currentExecutor;
                    } catch (Exception ignored) {
                        // Node is still down, discover another live node in the cluster
                    }
                }

                currentTarget = discoverLiveNode();
                Registry reg = LocateRegistry.getRegistry(currentTarget.ip, currentTarget.port);
                currentExecutor = (RemoteExecutorInterface) reg.lookup("Executor");
                return currentExecutor;
            } catch (Exception e) {
                System.err.println("[Connection Error] " + e.getMessage() + ". Retrying in 2 seconds...");
                currentExecutor = null;
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ignored) {}
            }
        }
        return currentExecutor;
    }

    private static void invalidateConnection() {
        currentExecutor = null;
    }

    public static String safeSubmitJob(Job<?> job) {
        while (true) {
            try {
                RemoteExecutorInterface exec = getConnectedExecutor();
                return exec.submitJob(job);
            } catch (RemoteException e) {
                System.err.println("[Client Failover] Node communication failed during submitJob (" + e.getMessage() + "). Failing over to another active node...");
                invalidateConnection();
                try {
                    Thread.sleep(1500);
                } catch (InterruptedException ignored) {}
            }
        }
    }

    public static Object safeGetJobResult(String jobId) {
        while (true) {
            try {
                RemoteExecutorInterface exec = getConnectedExecutor();
                return exec.getJobResult(jobId);
            } catch (RemoteException e) {
                System.err.println("[Client Failover] Node communication failed during getJobResult for " + jobId + " (" + e.getMessage() + "). Failing over to another active node...");
                invalidateConnection();
                try {
                    Thread.sleep(1500);
                } catch (InterruptedException ignored) {}
            }
        }
    }

    public static void main(String[] args) {
        try {
            // 1. Initial connection
            getConnectedExecutor();

            // 2. Create generic Job with Client ID and Request ID
            String clientId = "client-" + java.util.UUID.randomUUID().toString().substring(0, 8);
            long requestId = 1L;
            Job<Integer> computeJob = new Job<>(clientId, requestId, () -> {
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

            // 3. Submit the job with automatic failover
            String returnedId = safeSubmitJob(computeJob);
            System.out.println("Submitted Job ID: " + returnedId);

            // 4. Poll for result with automatic failover
            Object result = null;
            while (result == null) {
                result = safeGetJobResult(returnedId);
                if (result == null) {
                    System.out.println("Job " + returnedId + " still processing... polling again in 1 second");
                    Thread.sleep(1000);
                }
            }

            System.out.println("Job Result: " + result);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}