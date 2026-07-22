import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Client {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java Client <gatewayIp> <gatewayPort>");
            System.exit(1);
        }

        String ip = args[0];
        int port = Integer.parseInt(args[1]);

        try {
            // 1. Locate the RMI registry of a specific executor
            Registry registry = LocateRegistry.getRegistry(ip, port);
            RemoteExecutorInterface executor = (RemoteExecutorInterface) registry.lookup("Executor");

            System.out.println("Connected to Executor at " + ip + ":" + port);

            // 2. Create a generic Job (Using SerializableSupplier)
            String jobId = "job-" + System.currentTimeMillis();
            Job<Integer> computeJob = new Job<>(jobId, () -> {
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

            // 3. Submit the job
            String returnedId = executor.submitJob(computeJob);
            System.out.println("Submitted Job ID: " + returnedId);

            // 4. Poll for result
            Object result = null;
            while (result == null) {
                result = executor.getJobResult(returnedId);
                if (result == null) {
                    System.out.println("Waiting for job to finish...");
                    Thread.sleep(1000);
                }
            }

            System.out.println("Job Result: " + result);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
