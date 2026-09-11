import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Executor extends UnicastRemoteObject implements RemoteExecutorInterface {
    private static final long serialVersionUID = 1L;

    private final String nodeId;
    private final ClusterManager clusterManager;
    private final ExecutorService threadPool;
    private final AtomicInteger activeJobsCount;
    private final ScheduledExecutorService schedulerService;

    public Executor(String nodeId, ClusterManager clusterManager) throws RemoteException {
        this.nodeId = nodeId;
        this.clusterManager = clusterManager;
        this.threadPool = Executors.newFixedThreadPool(4);
        this.activeJobsCount = new AtomicInteger(0);
        this.schedulerService = Executors.newSingleThreadScheduledExecutor();

        //Avvio del loop di scheduling del Leader ogni secondo
        this.schedulerService.scheduleWithFixedDelay(this::schedulePendingJobs, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    public int getActiveJobsCount() {
        return activeJobsCount.get();
    }

    private boolean isLeader() {
        return clusterManager.isLeader();
    }

    @Override
    public VoteResponse requestVote(int term, String candidateId, int lastLogIndex, int lastLogTerm) throws RemoteException {
        return clusterManager.handleRequestVote(term, candidateId, lastLogIndex, lastLogTerm);
    }

    public boolean requestVote(int term, String candidateId) throws RemoteException {
        return clusterManager.handleRequestVote(term, candidateId);
    }

    @Override
    public AppendEntriesResponse appendEntries(int term, String leaderId, int prevLogIndex, int prevLogTerm,
                                              List<LogEntryJob> entries, int leaderCommit) throws RemoteException {
        return clusterManager.handleAppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    @Override
    public String submitJob(Job<?> job) throws RemoteException {
        if (!isLeader()) {
            String leaderId = clusterManager.getCurrentLeader();
            if (leaderId == null) {
                throw new RemoteException("No leader currently available (Election in progress). Please retry shortly.");
            }
            System.out.println("[" + nodeId + " Follower] Forwarding job submission to leader: " + leaderId);
            NodeInfo leaderInfo = clusterManager.getNodeInfo(leaderId);
            if (leaderInfo == null) {
                throw new RemoteException("Leader info unavailable for node " + leaderId + ". Please retry shortly.");
            }
            try {
                Registry reg = LocateRegistry.getRegistry(leaderInfo.getIpAddress(), leaderInfo.getPort());
                RemoteExecutorInterface remoteLeader = (RemoteExecutorInterface) reg.lookup("Executor");
                return remoteLeader.submitJob(job);
            } catch (Exception e) {
                throw new RemoteException("Failed to forward job submission to leader " + leaderId, e);
            }
        }

        if (job.getClientId() != null && job.getRequestId() > 0) {
            String existingJobId = clusterManager.getJobStateMachine().getExistingJobId(job.getClientId(), job.getRequestId());
            if (existingJobId != null) {
                System.out.println("[" + nodeId + " Leader] Deduplicated request (" + job.getClientId() + ", " + job.getRequestId() + ") -> returning existing jobId " + existingJobId);
                return existingJobId;
            }
        }

        if (job.getJobId() == null) {
            job.setJobId("job-" + this.nodeId + "-" + System.currentTimeMillis() + "-" + UUID.randomUUID().toString().substring(0, 8));
        }

        System.out.println("[" + nodeId + " Leader] Proposing CREATE_JOB in Raft log for " + job.getJobId());

        // Inserisce il job nel raft log
        int entryIndex = clusterManager.getRaftLog().append(
                clusterManager.getCurrentTerm(),
                LogEntryJob.Type.CREATE_JOB,
                job.getJobId(),
                job
        );

        // Inzio della replica a maggioranza
        clusterManager.triggerReplication();

        // Aspetta il commit
        boolean committed = clusterManager.waitForCommit(entryIndex, 5000);
        if (!committed) {
            throw new RemoteException("Consensus commit timeout for job " + job.getJobId());
        }

        // Parte lo scheduling appena è committato
        new Thread(this::schedulePendingJobs).start();

        return job.getJobId();
    }

    public synchronized void schedulePendingJobs() {
        if (!isLeader()) {
            return;
        }

        JobStateMachine sm = clusterManager.getJobStateMachine();
        Map<String, JobMetadata> allJobs = sm.getAllJobs();

        // Controlla se un nodo assegnatario di un job in corso è morto
        for (JobMetadata meta : allJobs.values()) {
            JobMetadata.State st = meta.getState();
            if (st == JobMetadata.State.ASSIGNED || st == JobMetadata.State.RUNNING) {
                String assignedNode = meta.getAssignedNodeId();
                if (assignedNode != null && !clusterManager.isNodeAlive(assignedNode)) {
                    System.out.println("[" + nodeId + " Leader Scheduler] Detected DEAD worker " + assignedNode +
                            " for job " + meta.getJobId() + ". Requeueing (Attempt: " + meta.getAttempt() + ")...");
                    clusterManager.getRaftLog().append(
                            clusterManager.getCurrentTerm(),
                            LogEntryJob.Type.REQUEUE_JOB,
                            meta.getJobId(),
                            null
                    );
                    clusterManager.triggerReplication();
                }
            }
        }

        //Assegna i job in stato SUBMITTED
        Set<NodeInfo> aliveNodes = clusterManager.getAliveNodes();
        if (aliveNodes.isEmpty()) {
            return;
        }

        // Calcola il carico effettivo di ciascun nodo attivo direttamente dalla State Machine
        Map<String, Integer> currentLoads = new HashMap<>();
        for (NodeInfo n : aliveNodes) {
            currentLoads.put(n.getNodeId(), 0);
        }
        for (JobMetadata meta : allJobs.values()) {
            if ((meta.getState() == JobMetadata.State.ASSIGNED || meta.getState() == JobMetadata.State.RUNNING)
                    && meta.getAssignedNodeId() != null) {
                currentLoads.computeIfPresent(meta.getAssignedNodeId(), (k, v) -> v + 1);
            }
        }

        for (JobMetadata meta : allJobs.values()) {
            if (meta.getState() == JobMetadata.State.SUBMITTED) {
                NodeInfo chosenNode = aliveNodes.stream()
                        .min(Comparator.comparingInt(n -> currentLoads.getOrDefault(n.getNodeId(), 0)))
                        .orElse(null);

                if (chosenNode != null) {
                    final String chosenNodeId = chosenNode.getNodeId();
                    final int attempt = meta.getAttempt();
                    final Job<?> jobToRun = meta.getJobObject();

                    System.out.println("[" + nodeId + " Leader Scheduler] Assigning job " + meta.getJobId() +
                            " to " + chosenNodeId + " (Current load: " + currentLoads.get(chosenNodeId) + ", Attempt: " + attempt + ")");


                    int assignIndex = clusterManager.getRaftLog().append(
                            clusterManager.getCurrentTerm(),
                            LogEntryJob.Type.ASSIGN_JOB,
                            meta.getJobId(),
                            new Object[]{ chosenNodeId, attempt }
                    );
                    clusterManager.triggerReplication();
                    currentLoads.put(chosenNodeId, currentLoads.get(chosenNodeId) + 1);

                    // Aspetta che avvenga il commit e poi contatta il worker designato per l'esecuzione
                    new Thread(() -> {
                        if (clusterManager.waitForCommit(assignIndex, 5000)) {
                            dispatchJobExecution(chosenNode, jobToRun, attempt);
                        } else {
                            System.err.println("[" + nodeId + " Leader Scheduler] Commit timeout for ASSIGN_JOB " + meta.getJobId());
                        }
                    }).start();
                }
            }
        }
    }

    private void dispatchJobExecution(NodeInfo targetNode, Job<?> job, int attempt) {
        if (targetNode.getNodeId().equals(this.nodeId)) {
            try {
                this.executeJob(job, attempt);
            } catch (RemoteException e) {
                e.printStackTrace();
            }
        } else {
            try {
                Registry reg = LocateRegistry.getRegistry(targetNode.getIpAddress(), targetNode.getPort());
                RemoteExecutorInterface remote = (RemoteExecutorInterface) reg.lookup("Executor");
                remote.executeJob(job, attempt);
            } catch (Exception e) {
                System.err.println("[" + nodeId + " Leader] Failed to dispatch job " + job.getJobId() +
                        " to " + targetNode.getNodeId() + ": " + e.getMessage());
            }
        }
    }

    @Override
    public void executeJob(Job<?> job, int attempt) throws RemoteException {
        job.setAttempt(attempt);
        activeJobsCount.incrementAndGet();

        threadPool.submit(() -> {
            try {
                System.out.println("[" + nodeId + " Worker] Starting execution of job " + job.getJobId() + " (Attempt: " + attempt + ")...");
                notifyJobStart(job.getJobId(), attempt);

                Object result = job.execute();

                System.out.println("[" + nodeId + " Worker] Successfully completed job " + job.getJobId() + " (Attempt: " + attempt + ")");
                sendCompletionToLeader(job.getJobId(), attempt, result, true, null);
            } catch (Throwable t) {
                System.err.println("[" + nodeId + " Worker] Error executing job " + job.getJobId() + ": " + t.getMessage());
                sendCompletionToLeader(job.getJobId(), attempt, null, false, t.getMessage());
            } finally {
                activeJobsCount.decrementAndGet();
            }
        });
    }

    @Override
    public void executeJob(Job<?> job) throws RemoteException {
        executeJob(job, job.getAttempt() > 0 ? job.getAttempt() : 1);
    }

    private void notifyJobStart(String jobId, int attempt) {
        if (isLeader()) {
            clusterManager.getRaftLog().append(clusterManager.getCurrentTerm(), LogEntryJob.Type.START_JOB, jobId, attempt);
            clusterManager.triggerReplication();
        }
    }

    private void sendCompletionToLeader(String jobId, int attempt, Object result, boolean success, String errorMsg) {
        if (isLeader()) {
            try {
                notifyJobCompletion(jobId, attempt, result, success, errorMsg);
            } catch (RemoteException ignored) {}
        } else {
            String leaderId = clusterManager.getCurrentLeader();
            if (leaderId != null) {
                NodeInfo leaderInfo = clusterManager.getNodeInfo(leaderId);
                if (leaderInfo != null) {
                    try {
                        Registry reg = LocateRegistry.getRegistry(leaderInfo.getIpAddress(), leaderInfo.getPort());
                        RemoteExecutorInterface remoteLeader = (RemoteExecutorInterface) reg.lookup("Executor");
                        remoteLeader.notifyJobCompletion(jobId, attempt, result, success, errorMsg);
                    } catch (Exception e) {
                        System.err.println("[" + nodeId + " Worker] Failed to send completion to leader " + leaderId + ": " + e.getMessage());
                    }
                }
            }
        }
    }

    @Override
    public void notifyJobCompletion(String jobId, int attempt, Object result, boolean success, String errorMsg) throws RemoteException {
        if (!isLeader()) {
            sendCompletionToLeader(jobId, attempt, result, success, errorMsg);
            return;
        }

        JobMetadata meta = clusterManager.getJobStateMachine().getJob(jobId);
        if (meta != null) {
            // Verifica attempt per evitare risposte obsolete da worker precedentemente considerati morti
            if (meta.getAttempt() != attempt) {
                System.out.println("[" + nodeId + " Leader] Ignoring stale completion for job " + jobId +
                        " (Received attempt: " + attempt + ", Current attempt: " + meta.getAttempt() + ")");
                return;
            }

            if (success) {
                System.out.println("[" + nodeId + " Leader] Proposing COMPLETE_JOB for " + jobId);
                clusterManager.getRaftLog().append(clusterManager.getCurrentTerm(), LogEntryJob.Type.COMPLETE_JOB, jobId, result);
            } else {
                System.out.println("[" + nodeId + " Leader] Proposing FAIL_JOB for " + jobId);
                clusterManager.getRaftLog().append(clusterManager.getCurrentTerm(), LogEntryJob.Type.FAIL_JOB, jobId, errorMsg);
            }
            clusterManager.triggerReplication();
        }
    }

    @Override
    public Object getJobResult(String jobId) throws RemoteException {
        JobMetadata meta = clusterManager.getJobStateMachine().getJob(jobId);
        if (meta == null) {
            return null;
        }

        if (meta.getState() == JobMetadata.State.COMPLETED) {
            return meta.getResult();
        }
        if (meta.getState() == JobMetadata.State.FAILED) {
            return "ERROR: " + meta.getErrorMessage();
        }
        return null;
    }

    @Override
    public Object getLocalJobResult(String jobId) throws RemoteException {
        return getJobResult(jobId);
    }

    @Override
    public void updateLeader(String leaderNodeId) throws RemoteException { }
}
