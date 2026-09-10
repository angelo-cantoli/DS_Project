import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.Random;

public class ClusterManager {
    public enum NodeState { FOLLOWER, CANDIDATE, LEADER }

    private static final long UDP_TIMEOUT = 15000;

    private final Map<String, NodeRecord> activeNodes = new ConcurrentHashMap<>();
    private final ScheduledExecutorService failureDetector = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService replicationScheduler = Executors.newSingleThreadScheduledExecutor();

    // Raft Consensus State
    private NodeState state = NodeState.FOLLOWER;
    private int currentTerm = 0;
    private String votedFor = null;
    private String currentLeader = null;
    private long lastLeaderHeartbeat = System.currentTimeMillis();
    private final String selfNodeId;
    private final int expectedClusterSize;
    private long electionTimeout;
    private final Random random = new Random();
    private final RaftLog raftLog;
    private final JobStateMachine jobStateMachine = new JobStateMachine();

    // Raft Leader Volatile State
    private final Map<String, Integer> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Integer> matchIndex = new ConcurrentHashMap<>();

    public ClusterManager(String selfNodeId, int expectedClusterSize) {
        this.selfNodeId = selfNodeId;
        this.expectedClusterSize = expectedClusterSize;
        this.raftLog = new RaftLog(selfNodeId);
        loadRaftState();
        applyCommittedEntries();
        resetElectionTimeout();
        failureDetector.scheduleAtFixedRate(this::checkFailures, 1, 1, TimeUnit.SECONDS);
        replicationScheduler.scheduleWithFixedDelay(this::replicateToFollowers, 1000, 1500, TimeUnit.MILLISECONDS);
    }

    public RaftLog getRaftLog() {
        return raftLog;
    }

    public JobStateMachine getJobStateMachine() {
        return jobStateMachine;
    }

    private void loadRaftState() {
        // Create a File object using the node's unique ID to avoid conflicts
        java.io.File file = new java.io.File(selfNodeId + "_raft.properties");
        // Check if a previous state exists on disk (node is recovering from a crash/restart)
        if (file.exists()) {
            // Use try-with-resources to ensure the input stream is automatically closed
            try (java.io.FileInputStream in = new java.io.FileInputStream(file)) {
                // Instantiate a Properties object to handle key-value pairs
                java.util.Properties props = new java.util.Properties();
                // Load the configuration from the input stream
                props.load(in);
                this.currentTerm = Integer.parseInt(props.getProperty("currentTerm", "0"));
                this.votedFor = props.getProperty("votedFor", null);
                if (this.votedFor != null && this.votedFor.isEmpty()) this.votedFor = null;
                System.out.println("[Raft WAL] Recovered State: Term=" + currentTerm + ", VotedFor=" + votedFor);
            } catch (Exception e) { e.printStackTrace(); }
        }
    }

    private synchronized void saveRaftState() {
        try {
            java.util.Properties props = new java.util.Properties();
            props.setProperty("currentTerm", String.valueOf(currentTerm));
            props.setProperty("votedFor", votedFor != null ? votedFor : "");
            
            try (java.io.FileOutputStream out = new java.io.FileOutputStream(selfNodeId + "_raft.properties")) {
                props.store(out, "Raft State WAL");
            }
        } catch (Exception e) { e.printStackTrace(); }
    }

    private void resetElectionTimeout() {
        // Random timeout between 5s and 10s to prevent split votes (like Raft)
        electionTimeout = 5000 + random.nextInt(5000);
    }

    private static class NodeRecord {
        final NodeInfo info;
        final long lastHeartbeat;
        NodeRecord(NodeInfo info, long lastHeartbeat) {
            this.info = info;
            this.lastHeartbeat = lastHeartbeat;
        }
    }

    public synchronized void isAlive(NodeInfo info) {
        activeNodes.put(info.getNodeId(), new NodeRecord(info, System.currentTimeMillis()));
        
        // If someone has a higher term, immediately step down
        if (info.getTerm() > currentTerm) {
            stepDown(info.getTerm());
        }
        
        // If the packet is from the acknowledged leader of the current term, reset timer
        if (info.isLeader() && info.getTerm() >= currentTerm) {
            currentLeader = info.getNodeId();
            if (currentTerm != info.getTerm()) {
                currentTerm = info.getTerm();
                saveRaftState();
            }
            state = NodeState.FOLLOWER;
            lastLeaderHeartbeat = System.currentTimeMillis();
        }
    }

    public synchronized void stepDown(int newTerm) {
        currentTerm = newTerm;
        state = NodeState.FOLLOWER;
        votedFor = null;
        saveRaftState();
        resetElectionTimeout();
    }

    public synchronized VoteResponse handleRequestVote(int candidateTerm, String candidateId, int candidateLastLogIndex, int candidateLastLogTerm) {
        if (candidateTerm > currentTerm) {
            stepDown(candidateTerm); // Someone with a higher term is running, respect it
        }
        
        boolean canVote = (candidateTerm == currentTerm) && (votedFor == null || votedFor.equals(candidateId));
        boolean logOk = raftLog.isUpToDate(candidateLastLogIndex, candidateLastLogTerm);

        if (canVote && logOk) {
            votedFor = candidateId;
            saveRaftState();
            lastLeaderHeartbeat = System.currentTimeMillis(); // grant vote and reset our own election timer
            System.out.println("[" + selfNodeId + " Raft Election] Vote GRANTED to " + candidateId + " for Term " + candidateTerm +
                    " (CandidateLog: [" + candidateLastLogIndex + ", Term " + candidateLastLogTerm + "], MyLog: [" + raftLog.getLastLogIndex() + ", Term " + raftLog.getLastLogTerm() + "])");
            return new VoteResponse(currentTerm, true);
        }

        System.out.println("[" + selfNodeId + " Raft Election] Vote DENIED to " + candidateId + " for Term " + candidateTerm +
                " (canVote=" + canVote + ", logOk=" + logOk + ", candidateLog=[" + candidateLastLogIndex + ", Term " + candidateLastLogTerm + "], myLog=[" + raftLog.getLastLogIndex() + ", Term " + raftLog.getLastLogTerm() + "])");
        return new VoteResponse(currentTerm, false); // Deny vote
    }

    public synchronized boolean handleRequestVote(int candidateTerm, String candidateId) {
        return handleRequestVote(candidateTerm, candidateId, 0, 0).isVoteGranted();
    }

    public synchronized void checkFailures() {
        long now = System.currentTimeMillis();
        
        // 1. Evict dead nodes
        activeNodes.entrySet().removeIf(entry -> {

            if (entry.getKey().equals(selfNodeId)) {
                return false;
            }

            boolean isDead = (now - entry.getValue().lastHeartbeat) > UDP_TIMEOUT;
            if (isDead) {
                System.out.println(entry.getKey() + " is dead");
                if (entry.getKey().equals(currentLeader)) {
                    currentLeader = null;
                }
            }
            return isDead;
        });

        // 2. Election Timeout Check
        if (state != NodeState.LEADER && (now - lastLeaderHeartbeat) > electionTimeout) {
            startElection();
        }
    }

    private void startElection() {
        synchronized(this) {
            state = NodeState.CANDIDATE;
            currentTerm++;
            votedFor = selfNodeId; // Vote for self
            saveRaftState();
            lastLeaderHeartbeat = System.currentTimeMillis();
            resetElectionTimeout();
        }
        
        System.out.println("\n--- Election Timeout! Starting election for Term " + currentTerm + " ---");
        
        final int termToRequest = currentTerm;
        final int lastLogIndex = raftLog.getLastLogIndex();
        final int lastLogTerm = raftLog.getLastLogTerm();
        // Calculate Quorum strictly based on expected cluster size to prevent Split-Brain!
        final int quorum = (expectedClusterSize / 2) + 1; 

        // Spin up a thread to request votes over RMI without blocking the heartbeat manager
        new Thread(() -> {
            int votes = 1; // Start with 1 because we voted for ourselves
            
            for (NodeRecord record : activeNodes.values()) {
                if (record.info.getNodeId().equals(selfNodeId)) continue;
                try {
                    Registry reg = LocateRegistry.getRegistry(record.info.getIpAddress(), record.info.getPort());
                    RemoteExecutorInterface stub = (RemoteExecutorInterface) reg.lookup("Executor");
                    // RMI Call passing candidate's log completeness
                    VoteResponse voteResp = stub.requestVote(termToRequest, selfNodeId, lastLogIndex, lastLogTerm);
                    if (voteResp != null) {
                        if (voteResp.getTerm() > termToRequest) {
                            synchronized (this) {
                                stepDown(voteResp.getTerm());
                            }
                            return;
                        }
                        if (voteResp.isVoteGranted()) {
                            votes++;
                        }
                    }
                } catch (Exception e) {
                    // Node unreachable, ignore
                }
            }
            
            synchronized(this) {
                if (state == NodeState.CANDIDATE && termToRequest == currentTerm) {
                    if (votes >= quorum) {
                        System.out.println("WON Election with " + votes + "/" + activeNodes.size() + " votes! I am LEADER for term " + currentTerm);
                        state = NodeState.LEADER;
                        currentLeader = selfNodeId;
                        initLeaderState();
                        triggerReplication();
                    } else {
                        System.out.println("Lost election. Got " + votes + " votes. Quorum needed: " + quorum);
                    }
                }
            }
        }).start();
    }

    public synchronized boolean isLeader() { return state == NodeState.LEADER; }
    public synchronized int getCurrentTerm() { return currentTerm; }
    public synchronized String getCurrentLeader() { return currentLeader; }

    public Set<NodeInfo> getAliveNodes() {
        return activeNodes.values().stream()
                .map(record -> record.info)
                .collect(Collectors.toSet());
    }

    public NodeInfo getNodeInfo(String nodeId) {
        NodeRecord record = activeNodes.get(nodeId);
        return record != null ? record.info : null;
    }

    public boolean isNodeAlive(String nodeId) {
        return activeNodes.containsKey(nodeId);
    }
    //DA VEDERE BENE
    public synchronized void updateLocalNodeLoad(int currentLoad, int term, boolean isLeader) {
        NodeRecord record = activeNodes.get(selfNodeId);
        if (record != null) {
            // Update the existing record with the fresh load while keeping IP and Port intact
            NodeInfo freshInfo = new NodeInfo(
                    selfNodeId,
                    record.info.getIpAddress(),
                    record.info.getPort(),
                    currentLoad,
                    term,
                    isLeader
            );
            activeNodes.put(selfNodeId, new NodeRecord(freshInfo, System.currentTimeMillis()));
        } else {
            // Fallback if self record isn't in the map yet (e.g., right after election)
            // Note: 127.0.0.1 and port 1099 are safe defaults if fallback is ever triggered
            NodeInfo freshInfo = new NodeInfo(
                    selfNodeId,
                    "127.0.0.1",
                    1099,
                    currentLoad,
                    term,
                    isLeader
            );
            activeNodes.put(selfNodeId, new NodeRecord(freshInfo, System.currentTimeMillis()));
        }
    }

    private synchronized void initLeaderState() {
        nextIndex.clear();
        matchIndex.clear();
        int lastIndex = raftLog.getLastLogIndex();
        for (NodeRecord record : activeNodes.values()) {
            String peerId = record.info.getNodeId();
            if (!peerId.equals(selfNodeId)) {
                nextIndex.put(peerId, lastIndex + 1);
                matchIndex.put(peerId, 0);
            }
        }
    }

    public void triggerReplication() {
        new Thread(this::replicateToFollowers).start();
    }

    public synchronized void replicateToFollowers() {
        if (state != NodeState.LEADER) return;

        final int term = currentTerm;
        final int leaderCommit = raftLog.getCommitIndex();

        for (NodeRecord record : activeNodes.values()) {
            final String peerId = record.info.getNodeId();
            if (peerId.equals(selfNodeId)) continue;

            final int pNextIndex = nextIndex.getOrDefault(peerId, raftLog.getLastLogIndex() + 1);
            final int prevLogIndex = pNextIndex - 1;
            final LogEntryJob prevEntry = raftLog.getEntry(prevLogIndex);
            final int prevLogTerm = prevEntry != null ? prevEntry.getTerm() : 0;
            final List<LogEntryJob> entriesToSend = raftLog.getEntriesFrom(pNextIndex);

            new Thread(() -> {
                try {
                    Registry reg = LocateRegistry.getRegistry(record.info.getIpAddress(), record.info.getPort());
                    RemoteExecutorInterface stub = (RemoteExecutorInterface) reg.lookup("Executor");
                    AppendEntriesResponse resp = stub.appendEntries(term, selfNodeId, prevLogIndex, prevLogTerm, entriesToSend, leaderCommit);

                    if (resp != null) {
                        synchronized (ClusterManager.this) {
                            if (resp.getTerm() > currentTerm) {
                                stepDown(resp.getTerm());
                                return;
                            }

                            if (state != NodeState.LEADER || term != currentTerm) {
                                return;
                            }

                            if (resp.isSuccess()) {
                                matchIndex.put(peerId, resp.getMatchIndex());
                                nextIndex.put(peerId, resp.getMatchIndex() + 1);
                                checkQuorumCommit();
                            } else {
                                int currentNext = nextIndex.getOrDefault(peerId, 1);
                                if (currentNext > 1) {
                                    nextIndex.put(peerId, currentNext - 1);
                                    triggerReplication();
                                }
                            }
                        }
                    }
                } catch (Exception ignored) {
                }
            }).start();
        }
    }

    private synchronized void checkQuorumCommit() {
        if (state != NodeState.LEADER) return;

        int medianCommit = raftLog.getCommitIndex();
        int lastIndex = raftLog.getLastLogIndex();
        final int quorum = (expectedClusterSize / 2) + 1;

        for (int N = lastIndex; N > raftLog.getCommitIndex(); N--) {
            LogEntryJob entry = raftLog.getEntry(N);
            if (entry == null || entry.getTerm() != currentTerm) {
                continue;
            }

            int count = 1; // Self count
            for (String peerId : matchIndex.keySet()) {
                if (matchIndex.getOrDefault(peerId, 0) >= N) {
                    count++;
                }
            }

            if (count >= quorum) {
                medianCommit = N;
                break;
            }
        }

        if (medianCommit > raftLog.getCommitIndex()) {
            raftLog.setCommitIndex(medianCommit);
            System.out.println("[" + selfNodeId + " Leader] Advanced commitIndex to " + medianCommit + " (Quorum: " + quorum + " reached)");
            applyCommittedEntries();
            notifyAll(); // Unblock waitForCommit
        }
    }

    public synchronized boolean waitForCommit(int index, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (raftLog.getCommitIndex() < index) {
            if (state != NodeState.LEADER) {
                return false;
            }
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                return false;
            }
            try {
                wait(Math.min(remaining, 300));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }

    public synchronized AppendEntriesResponse handleAppendEntries(int term, String leaderId, int prevLogIndex, int prevLogTerm,
                                                                   List<LogEntryJob> entries, int leaderCommit) {
        // 1. Reply false if term < currentTerm
        if (term < currentTerm) {
            System.out.println("[" + selfNodeId + " Raft] Rejected AppendEntries from " + leaderId + ": stale term " + term + " < " + currentTerm);
            return new AppendEntriesResponse(currentTerm, false, raftLog.getLastLogIndex());
        }

        // 2. If term >= currentTerm, acknowledge leader and reset election timer
        if (term > currentTerm) {
            stepDown(term);
        } else if (state == NodeState.CANDIDATE) {
            state = NodeState.FOLLOWER;
            resetElectionTimeout();
        }

        currentLeader = leaderId;
        lastLeaderHeartbeat = System.currentTimeMillis(); // Reset election timer upon valid AppendEntries!

        // 3. Consistency check: reply false if log doesn't contain an entry at prevLogIndex matching prevLogTerm
        if (prevLogIndex > 0) {
            LogEntryJob prevEntry = raftLog.getEntry(prevLogIndex);
            if (prevEntry == null || prevEntry.getTerm() != prevLogTerm) {
                System.out.println("[" + selfNodeId + " Raft] Inconsistency with leader " + leaderId +
                        " at prevLogIndex=" + prevLogIndex + " (localEntry=" + prevEntry + ", expectedTerm=" + prevLogTerm + ")");
                return new AppendEntriesResponse(currentTerm, false, raftLog.getLastLogIndex());
            }
        }

        // 4. Append new entries and resolve conflicts
        if (entries != null && !entries.isEmpty()) {
            for (LogEntryJob newEntry : entries) {
                int idx = newEntry.getIndex();
                LogEntryJob existing = raftLog.getEntry(idx);
                if (existing != null) {
                    if (existing.getTerm() != newEntry.getTerm()) {
                        // Conflict! Truncate from idx onward
                        raftLog.truncate(idx);
                        raftLog.append(newEntry);
                    }
                } else {
                    raftLog.append(newEntry);
                }
            }
        }

        // 5. Update commitIndex
        if (leaderCommit > raftLog.getCommitIndex()) {
            int newCommit = Math.min(leaderCommit, raftLog.getLastLogIndex());
            raftLog.setCommitIndex(newCommit);
            applyCommittedEntries();
        }

        return new AppendEntriesResponse(currentTerm, true, raftLog.getLastLogIndex());
    }

    public synchronized void applyCommittedEntries() {
        int commitIndex = raftLog.getCommitIndex();
        int lastApplied = raftLog.getLastApplied();
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntryJob entry = raftLog.getEntry(lastApplied);
            if (entry != null && entry.getType() != null) {
                jobStateMachine.apply(entry);
                System.out.println("[" + selfNodeId + " StateMachine] Applied entry index=" + lastApplied +
                        ", type=" + entry.getType() + ", jobId=" + entry.getJobId() +
                        ", stateMachineAppliedIndex=" + jobStateMachine.getLastAppliedIndex());
            }
            raftLog.setLastApplied(lastApplied);
        }
    }

    public void stop() {
        failureDetector.shutdown();
        replicationScheduler.shutdown();
    }
}
