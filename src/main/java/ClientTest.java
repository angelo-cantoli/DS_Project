import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;

public class ClientTest {

    // Number of jobs to submit
    private static final int NUM_JOBS = 10;

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
            // -------------------------------------------------
            // 1. Initial connection
            // -------------------------------------------------
            getConnectedExecutor();

            // -------------------------------------------------
            // 2. Submit multiple jobs
            // -------------------------------------------------
            System.out.println("\nSubmitting " + NUM_JOBS + " jobs...\n");

            Map<String, Object> results = new HashMap<>();
            long submissionStartTime = System.currentTimeMillis();

            String clientId = "test-client-" + java.util.UUID.randomUUID().toString().substring(0, 8);

            for (int i = 0; i < NUM_JOBS; i++) {
                final int jobNumber = i;

                Job<Integer> computeJob = new Job<>(
                        clientId,
                        (long) (i + 1),
                        () -> {
                            System.out.println("[Job " + jobNumber + "] Started");
                            int sum = 0;
                            for (int j = 1; j <= 1000; j++) {
                                sum += j;
                                try {
                                    Thread.sleep(15);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                            System.out.println("[Job " + jobNumber + "] Completed");
                            return sum;
                        }
                );

                // Invio con failover automatico: sopravvive al crash del nodo contattato
                String returnedId = safeSubmitJob(computeJob);
                results.put(returnedId, null);

                System.out.println("Submitted Job ID: " + returnedId);

                // Pausa tra una sottomissione e l'altra per osservare i log
                System.out.println("Waiting 2 seconds before next job...");
                Thread.sleep(2000);
            }

            long submissionEndTime = System.currentTimeMillis();
            System.out.println("\nAll jobs submitted in " + (submissionEndTime - submissionStartTime) + " ms");

            // -------------------------------------------------
            // 3. Poll for results
            // -------------------------------------------------
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

                    // Interrogazione con failover automatico
                    Object result = safeGetJobResult(jobId);
                    if (result != null) {
                        entry.setValue(result);
                        completedJobs++;
                        System.out.println("Job " + jobId + " completed. Result: " + result);
                    }
                }

                System.out.println("Progress: " + completedJobs + "/" + NUM_JOBS + " jobs completed");

                if (completedJobs < NUM_JOBS) {
                    System.out.println("Sleeping 2 seconds before next polling cycle...");
                    Thread.sleep(2000);
                }
            }

            long executionEndTime = System.currentTimeMillis();

            // -------------------------------------------------
            // 4. Final statistics
            // -------------------------------------------------
            System.out.println("\n======================================");
            System.out.println("STRESS TEST COMPLETED");
            System.out.println("======================================");
            System.out.println("Total jobs: " + NUM_JOBS);
            System.out.println("Submission time: " + (submissionEndTime - submissionStartTime) + " ms");
            System.out.println("Total execution time: " + (executionEndTime - executionStartTime) + " ms");
            System.out.println("Total test time: " + (executionEndTime - submissionStartTime) + " ms");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}