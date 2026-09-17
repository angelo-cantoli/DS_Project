import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;

public class ClientTest {

    private static final int NUM_JOBS = 10;

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
            System.out.println("\nSubmitting " + NUM_JOBS + " jobs...\n");

            Map<String, Object> results = new HashMap<>();
            long submissionStartTime = System.currentTimeMillis();

            for (int i = 0; i < NUM_JOBS; i++) {
                final int jobNumber = i;
                Job<Integer> computeJob = new Job<>(() -> {
                    System.out.println("[Job " + jobNumber + "] Started");
                    int sum = 0;
                    for (int j = 1; j <= 1000; j++) {
                        sum += j;
                        try { Thread.sleep(15); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    }
                    System.out.println("[Job " + jobNumber + "] Completed");
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
                
                results.put(returnedId, null);
                System.out.println("Submitted Job ID: " + returnedId);
                
                System.out.println("Waiting 4 seconds for cluster state synchronization...");
                Thread.sleep(4000);
            }

            long submissionEndTime = System.currentTimeMillis();
            System.out.println("\nAll jobs submitted in " + (submissionEndTime - submissionStartTime) + " ms");
            System.out.println("\nWaiting for all jobs to complete...\n");

            int completedJobs = 0;
            long executionStartTime = System.currentTimeMillis();

            while (completedJobs < NUM_JOBS) {
                completedJobs = 0;
                for (Map.Entry<String, Object> entry : results.entrySet()) {
                    String jobId = entry.getKey();
                    if (entry.getValue() != null) {
                        completedJobs++;
                        continue;
                    }

                    Object result = null;
                    try {
                        result = executor.getJobResult(jobId);
                    } catch (RemoteException e) {
                        System.err.println("Connection lost while polling! Reconnecting to a new node...");
                        executor = getExecutorStub(); // Reconnect and retry on next loop iteration
                        break; // Break the for loop to start over with the new executor
                    }

                    if (result != null) {
                        entry.setValue(result);
                        completedJobs++;
                        System.out.println("Job " + jobId + " completed. Result: " + result);
                    }
                }

                System.out.println("Progress: " + completedJobs + "/" + NUM_JOBS + " jobs completed");
                if (completedJobs < NUM_JOBS) {
                    System.out.println("Sleeping 3 seconds before next polling cycle...");
                    Thread.sleep(3000);
                }
            }

            long executionEndTime = System.currentTimeMillis();

            System.out.println("\n======================================");
            System.out.println("STRESS TEST COMPLETED");
            System.out.println("======================================");
            System.out.println("Total jobs: " + NUM_JOBS);
            System.out.println("Submission time: " + (submissionEndTime - submissionStartTime) + " ms");
            System.out.println("Total execution time: " + (executionEndTime - executionStartTime) + " ms");
            System.out.println("Total test time: " + (executionEndTime - submissionStartTime) + " ms");

        } catch (java.net.SocketTimeoutException e) {
            System.err.println("Timeout: No active nodes discovered. Is the cluster running?");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}