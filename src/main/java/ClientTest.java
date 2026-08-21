import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
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

    // Method to dynamically discover a live node using UDP Multicast
    private static NodeCoordinates discoverLiveNode() throws Exception {
        System.out.println(
                "Listening for cluster heartbeats on 230.0.0.0:4446..."
        );

        try (MulticastSocket socket = new MulticastSocket(4446)) {
            InetAddress group = InetAddress.getByName("230.0.0.0");

            socket.joinGroup(group);

            // Avoid infinite blocking if the cluster is completely down
            socket.setSoTimeout(10000);

            byte[] buffer = new byte[256];
            DatagramPacket packet =
                    new DatagramPacket(buffer, buffer.length);

            // Wait for the first heartbeat from any active node
            socket.receive(packet);

            // Parse:
            // senderId, ip, port, activeJobs, term, isLeader
            String payload = new String(
                    packet.getData(),
                    0,
                    packet.getLength()
            );

            String[] parts = payload.split(",");

            String targetIp = parts[1];
            int targetPort = Integer.parseInt(parts[2]);

            socket.leaveGroup(group);

            return new NodeCoordinates(targetIp, targetPort);
        }
    }

    public static void main(String[] args) {

        try {
            // -------------------------------------------------
            // 1. Discover an active node dynamically
            // -------------------------------------------------

            NodeCoordinates targetNode = discoverLiveNode();

            System.out.println(
                    "Discovered and connecting to Executor at "
                            + targetNode.ip
                            + ":"
                            + targetNode.port
            );

            // -------------------------------------------------
            // 2. Locate the RMI registry
            // -------------------------------------------------

            Registry registry = LocateRegistry.getRegistry(
                    targetNode.ip,
                    targetNode.port
            );

            RemoteExecutorInterface executor =
                    (RemoteExecutorInterface)
                            registry.lookup("Executor");

            // -------------------------------------------------
            // 3. Submit multiple jobs
            // -------------------------------------------------

            System.out.println(
                    "\nSubmitting " + NUM_JOBS + " jobs...\n"
            );

            // Map:
            // Job ID -> result
            Map<String, Object> results = new HashMap<>();

            long submissionStartTime =
                    System.currentTimeMillis();

            for (int i = 0; i < NUM_JOBS; i++) {

                final int jobNumber = i;

                String jobId =
                        "stress-job-"
                                + jobNumber
                                + "-"
                                + System.currentTimeMillis();

                Job<Integer> computeJob =
                        new Job<>(
                                jobId,
                                () -> {

                                    System.out.println(
                                            "[Job "
                                                    + jobNumber
                                                    + "] Started"
                                    );

                                    int sum = 0;

                                    // Simulate computation
                                    for (int j = 1; j <= 1000; j++) {
                                        sum += j;

                                        try {
                                            Thread.sleep(15);
                                        } catch (
                                                InterruptedException e
                                        ) {
                                            Thread.currentThread()
                                                    .interrupt();
                                        }
                                    }

                                    System.out.println(
                                            "[Job "
                                                    + jobNumber
                                                    + "] Completed"
                                    );

                                    return sum;
                                }
                        );

                String returnedId =
                        executor.submitJob(computeJob);

                results.put(returnedId, null);

                System.out.println(
                        "Submitted Job ID: " + returnedId
                );
                // --- NEW CODE: Pause before submitting the next job ---
                // Wait 4 seconds to allow UDP heartbeats to propagate the updated load state to the Leader
                System.out.println("Waiting 4 seconds for cluster state synchronization...");
                Thread.sleep(4000);
            }

            long submissionEndTime =
                    System.currentTimeMillis();

            System.out.println(
                    "\nAll jobs submitted in "
                            + (submissionEndTime
                            - submissionStartTime)
                            + " ms"
            );

            // -------------------------------------------------
            // 4. Poll for results
            // -------------------------------------------------

            System.out.println(
                    "\nWaiting for all jobs to complete...\n"
            );

            int completedJobs = 0;

            long executionStartTime =
                    System.currentTimeMillis();

            while (completedJobs < NUM_JOBS) {

                completedJobs = 0;

                for (Map.Entry<String, Object> entry
                        : results.entrySet()) {

                    String jobId = entry.getKey();

                    // Skip already completed jobs
                    if (entry.getValue() != null) {
                        completedJobs++;
                        continue;
                    }

                    Object result =
                            executor.getJobResult(jobId);

                    if (result != null) {

                        entry.setValue(result);

                        completedJobs++;

                        System.out.println(
                                "Job "
                                        + jobId
                                        + " completed. Result: "
                                        + result
                        );
                    }
                }

                System.out.println(
                        "Progress: "
                                + completedJobs
                                + "/"
                                + NUM_JOBS
                                + " jobs completed"
                );

                if (completedJobs < NUM_JOBS) {
                    System.out.println("Sleeping 3 seconds before next polling cycle...");
                    Thread.sleep(3000);
                }
            }

            long executionEndTime =
                    System.currentTimeMillis();

            // -------------------------------------------------
            // 5. Final statistics
            // -------------------------------------------------

            System.out.println(
                    "\n======================================"
            );

            System.out.println(
                    "STRESS TEST COMPLETED"
            );

            System.out.println(
                    "======================================"
            );

            System.out.println(
                    "Total jobs: " + NUM_JOBS
            );

            System.out.println(
                    "Submission time: "
                            + (submissionEndTime
                            - submissionStartTime)
                            + " ms"
            );

            System.out.println(
                    "Total execution time: "
                            + (executionEndTime
                            - executionStartTime)
                            + " ms"
            );

            System.out.println(
                    "Total test time: "
                            + (executionEndTime
                            - submissionStartTime)
                            + " ms"
            );

        } catch (java.net.SocketTimeoutException e) {

            System.err.println(
                    "Timeout: No active nodes discovered. "
                            + "Is the cluster running?"
            );

        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}