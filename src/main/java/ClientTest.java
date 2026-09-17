import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

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

    // Sostituire con gli IP reali che usi in Main.java
    private static final List<String> CLUSTER_IPS = Arrays.asList("172.20.10.2", "172.20.10.3", "172.20.10.4");

    // Method to dynamically discover a live node using RMI race
    private static NodeCoordinates discoverLiveNode() {
        while (true) {
            System.out.println("[Client] Contatto il cluster per stabilire una connessione...");
            
            ExecutorService executor = Executors.newFixedThreadPool(CLUSTER_IPS.size());
            CompletionService<NodeCoordinates> completionService = new ExecutorCompletionService<>(executor);

            for (String ip : CLUSTER_IPS) {
                completionService.submit(() -> {
                    Registry reg = LocateRegistry.getRegistry(ip, 1099);
                    reg.lookup("Executor"); 
                    return new NodeCoordinates(ip, 1099);
                });
            }

            try {
                Future<NodeCoordinates> fastestResponse = completionService.take(); 
                NodeCoordinates winner = fastestResponse.get();
                
                System.out.println("[Client] Connessione stabilita con successo al nodo: " + winner.ip);
                return winner;
            } catch (Exception e) {
                System.err.println("[Client] Nessun nodo disponibile. Nuovo tentativo in 2 secondi...");
                try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
            } finally {
                executor.shutdownNow();
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