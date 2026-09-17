import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Map;
import java.util.Set;
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
    private Executor executor;

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

    public ClusterManager(String selfNodeId, int expectedClusterSize) {
        this.selfNodeId = selfNodeId;
        this.expectedClusterSize = expectedClusterSize;
        loadRaftState();
        resetElectionTimeout();
        failureDetector.scheduleAtFixedRate(this::checkFailures, 1, 1, TimeUnit.SECONDS);
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
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

    public synchronized boolean handleRequestVote(int candidateTerm, String candidateId) {
        if (candidateTerm > currentTerm) {
            stepDown(candidateTerm); // Someone with a higher term is running, respect it
        }
        
        if (candidateTerm == currentTerm && (votedFor == null || votedFor.equals(candidateId))) {
            votedFor = candidateId;
            saveRaftState();
            lastLeaderHeartbeat = System.currentTimeMillis(); // grant vote and reset our own election timer
            return true;
        }
        return false; // Deny vote
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
                if (this.executor != null) {
                    this.executor.handleNodeFailure(entry.getKey());
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
                    // RMI Call
                    if (stub.requestVote(termToRequest, selfNodeId)) {
                        votes++;
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
                        if (executor != null) {
                            executor.rebuildGlobalState();
                        }
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

    public void stop() {
        failureDetector.shutdown();
    }
}
