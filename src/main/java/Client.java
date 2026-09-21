import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

public class Client {

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

    //WRITE IPS ON CONFIG
    private static final List<String> CLUSTER_IPS = Config.getClusterIps();

    // Dynamically discover a live node iterating over ports
    private static NodeCoordinates discoverLiveNode() {
        while (true) {
            System.out.println("[Client] Contatto il cluster per stabilire una connessione...");

            ExecutorService executor = Executors.newFixedThreadPool(CLUSTER_IPS.size() * 7);
            CompletionService<NodeCoordinates> completionService = new ExecutorCompletionService<>(executor);

            int totalTasks = 0;
            for (String ip : CLUSTER_IPS) {
                for (int port = 1099; port <= 1105; port++) {
                    final int p = port;
                    completionService.submit(() -> {
                        Registry reg = LocateRegistry.getRegistry(ip, p);
                        reg.lookup("Executor"); 
                        return new NodeCoordinates(ip, p);
                    });
                    totalTasks++;
                }
            }

            try {
                for (int i = 0; i < totalTasks; i++) {
                    Future<NodeCoordinates> response = completionService.poll(3, TimeUnit.SECONDS);
                    
                    if (response == null) {
                        throw new Exception("Timeout: i nodi non rispondono (IP errati o Firewall attivo?)");
                    }
                    
                    try {
                        NodeCoordinates winner = response.get();
                        System.out.println("[Client] Connessione stabilita con successo al nodo: " + winner.ip);
                        return winner;
                    } catch (ExecutionException e) {
                        System.err.println("[DEBUG] Tentativo fallito verso un IP. Causa: " + e.getCause().getMessage());
                    }
                }
                
                throw new Exception("Tutti i nodi indicati nella lista sono offline o irraggiungibili.");
                
            } catch (Exception e) {
                System.err.println("[Client] " + e.getMessage() + " Riprovo in 2 secondi...");
                try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
            } finally {
                executor.shutdownNow();
            }
        }
    }

    private static RemoteExecutorInterface getConnectedExecutor() {
        while (currentExecutor == null) {
            try {
                if (currentTarget != null) {
                    try {
                        Registry reg = LocateRegistry.getRegistry(currentTarget.ip, currentTarget.port);
                        currentExecutor = (RemoteExecutorInterface) reg.lookup("Executor");
                        return currentExecutor;
                    } catch (Exception ignored) {
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
            // Initial connection
            getConnectedExecutor();

            // Create generic job with client and request IDs
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

            // Submit the job
            String returnedId = safeSubmitJob(computeJob);
            System.out.println("Submitted Job ID: " + returnedId);

            // Poll for result
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