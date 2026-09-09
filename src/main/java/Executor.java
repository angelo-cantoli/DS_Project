import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.concurrent.*;
import java.util.Set;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

public class Executor extends UnicastRemoteObject implements RemoteExecutorInterface {
    private final String nodeId;
    private final ClusterManager clusterManager;
    private ConcurrentMap<String, Object> jobResults;
    private ExecutorService threadPool;
    private AtomicInteger activeJobsCount;
    private final java.io.File walDir;

    public Executor(String nodeId, ClusterManager clusterManager) throws RemoteException {
        this.nodeId = nodeId;
        this.clusterManager = clusterManager;
        this.jobResults = new ConcurrentHashMap<>();
        this.threadPool = Executors.newFixedThreadPool(4); 
        this.activeJobsCount = new AtomicInteger(0);
        
        this.walDir = new java.io.File("wal_" + nodeId);
        if (!this.walDir.exists()) {
            this.walDir.mkdirs();
        }
        recoverJobsFromWAL();
    }

    private void recoverJobsFromWAL() {
        java.io.File[] files = walDir.listFiles();
        if (files == null) return;
        
        // 1. Load results first
        for (java.io.File file : files) {
            if (file.getName().endsWith(".result")) {
                String jobId = file.getName().replace(".result", "");
                try (java.io.ObjectInputStream in = new java.io.ObjectInputStream(new java.io.FileInputStream(file))) {
                    Object result = in.readObject();
                    jobResults.put(jobId, result);
                    System.out.println("[Executor WAL] Recovered completed job result: " + jobId);
                } catch (Exception e) {}
            }
        }
        
        // 2. Load unfinished jobs
        for (java.io.File file : files) {
            if (file.getName().endsWith(".job")) {
                String jobId = file.getName().replace(".job", "");
                if (!jobResults.containsKey(jobId)) {
                    try (java.io.ObjectInputStream in = new java.io.ObjectInputStream(new java.io.FileInputStream(file))) {
                        Job<?> job = (Job<?>) in.readObject();
                        System.out.println("[Executor WAL] Recovered unfinished job: " + jobId + ". Resuming...");
                        executeJob(job, true); // true = already in WAL
                    } catch (Exception e) {}
                }
            }
        }
    }

    public int getActiveJobsCount() {
        return activeJobsCount.get();
    }

    private boolean isLeader() {
        return clusterManager.isLeader();
    }

    @Override
    public boolean requestVote(int term, String candidateId) throws RemoteException {
        // Forward RMI consensus requests to the local ClusterManager logic
        return clusterManager.handleRequestVote(term, candidateId);
    }

    @Override
    public String submitJob(Job<?> job) throws RemoteException {
        if (job.getJobId() == null) {
            job.setJobId("job-" + this.nodeId + "-" + java.util.UUID.randomUUID().toString());
        }
        System.out.println("Submitting job: " + job.getJobId());

        if (isLeader()) {
            clusterManager.updateLocalNodeLoad(getActiveJobsCount(), clusterManager.getCurrentTerm(), true);
            Set<NodeInfo> nodes = clusterManager.getAliveNodes();

            // --- STAMPA DI VERIFICA ---
            System.out.println("--- [CHECK LOAD STREAM] ---");
            for (NodeInfo n : nodes) {
                System.out.println("Node ID nella mappa: " + n.getNodeId() + " | ActiveJobs visti dallo stream: " + n.getActiveJobs());
            }
            System.out.println("Mio nodeId locale: " + this.nodeId + " | Miei job reali attivi: " + getActiveJobsCount());

            if (nodes.isEmpty()) {
                executeJob(job);
                return job.getJobId();
            }
            
            if (nodes.isEmpty()) {
                executeJob(job);
                return job.getJobId();
            }

            NodeInfo chosenNode = nodes.stream()
                .min(Comparator.comparingInt(NodeInfo::getActiveJobs))
                .orElse(null);
            
            System.out.println("Leader assigning job to node: " + chosenNode.getNodeId());

            if (chosenNode.getNodeId().equals(this.nodeId)) {
                executeJob(job);
            } else {
                try {
                    Registry registry = LocateRegistry.getRegistry(chosenNode.getIpAddress(), chosenNode.getPort());
                    RemoteExecutorInterface remoteExec = (RemoteExecutorInterface) registry.lookup("Executor");
                    remoteExec.executeJob(job);
                } catch (Exception e) {
                    System.err.println("Failed to forward job to " + chosenNode.getNodeId() + ". Executing locally.");
                    executeJob(job);
                }
            }
        } else {
            // Forward to Leader
            String leaderId = clusterManager.getCurrentLeader();
            if (leaderId == null) {
                throw new RemoteException("No leader currently available (Election in progress). Please retry in a few seconds.");
            }
            System.out.println("Forwarding job to leader: " + leaderId);
            NodeInfo leaderInfo = clusterManager.getNodeInfo(leaderId);
            try {
                Registry registry = LocateRegistry.getRegistry(leaderInfo.getIpAddress(), leaderInfo.getPort());
                RemoteExecutorInterface remoteLeader = (RemoteExecutorInterface) registry.lookup("Executor");
                return remoteLeader.submitJob(job);
            } catch (Exception e) {
                throw new RemoteException("Failed to forward job to leader", e);
            }
        }
        return job.getJobId();
    }

    @Override
    public void executeJob(Job<?> job) throws RemoteException {
        executeJob(job, false);
    }
    
    private void executeJob(Job<?> job, boolean isRecovered) {
        if (!isRecovered) {
            // Write to WAL before executing to guarantee Crash Recovery!
            try (java.io.ObjectOutputStream out = new java.io.ObjectOutputStream(new java.io.FileOutputStream(new java.io.File(walDir, job.getJobId() + ".job")))) {
                out.writeObject(job);
            } catch (Exception e) { e.printStackTrace(); }
        }

        activeJobsCount.incrementAndGet();
        threadPool.submit(() -> {
            try {
                Object result = job.execute();
                jobResults.put(job.getJobId(), result);
                
                // Write result to WAL to survive future crashes
                try (java.io.ObjectOutputStream out = new java.io.ObjectOutputStream(new java.io.FileOutputStream(new java.io.File(walDir, job.getJobId() + ".result")))) {
                    out.writeObject(result);
                } catch (Exception e) { e.printStackTrace(); }
                
                System.out.println("Job " + job.getJobId() + " completed.");
            } catch (Exception e) {
                jobResults.put(job.getJobId(), "ERROR: " + e.getMessage());
            } finally {
                activeJobsCount.decrementAndGet();
            }
        });
    }

    @Override
    public Object getJobResult(String jobId) throws RemoteException {
        Object result = jobResults.get(jobId);
        if (result != null) return result;

        if (!isLeader()) {
            String leaderId = clusterManager.getCurrentLeader();
            if (leaderId != null) {
                NodeInfo leaderInfo = clusterManager.getNodeInfo(leaderId);
                try {
                    Registry registry = LocateRegistry.getRegistry(leaderInfo.getIpAddress(), leaderInfo.getPort());
                    RemoteExecutorInterface remoteLeader = (RemoteExecutorInterface) registry.lookup("Executor");
                    return remoteLeader.getJobResult(jobId);
                } catch (Exception e) {
                    return null;
                }
            }
        } else {
            // I am the Leader: Scatter-gather results safely using local-only queries
            for (NodeInfo node : clusterManager.getAliveNodes()) {
                if (node.getNodeId().equals(this.nodeId)) continue;
                try {
                    Registry registry = LocateRegistry.getRegistry(node.getIpAddress(), node.getPort());
                    RemoteExecutorInterface remoteExec = (RemoteExecutorInterface) registry.lookup("Executor");

                    // --- FIX: Call getLocalJobResult to prevent the Ping-Pong recursion loop ---
                    Object remoteResult = remoteExec.getLocalJobResult(jobId);

                    if (remoteResult != null) return remoteResult;
                } catch (Exception e) {

                }
            }
        }
        return null;
    }

    @Override
    public Object getLocalJobResult(String jobId) throws RemoteException {
        // Returns the result directly from local memory without triggering any forwarding or recursion
        return jobResults.get(jobId);
    }

    @Override
    public void updateLeader(String leaderNodeId) throws RemoteException { }
}
