import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ClusterManager implements ClusterStatus {

    private static final long TIMEOUT = 6000;

    private final Map<String, NodeRecord> activeNodes = new ConcurrentHashMap<>();
    private final ScheduledExecutorService failureDetector = Executors.newSingleThreadScheduledExecutor();

    public ClusterManager() {
        failureDetector.scheduleAtFixedRate(this::checkFailures, 5, 5, TimeUnit.SECONDS);
    }

    private static class NodeRecord {
        final NodeInfo info;
        final long lastHeartbeat;
        NodeRecord(NodeInfo info, long lastHeartbeat) {
            this.info = info;
            this.lastHeartbeat = lastHeartbeat;
        }
    }

    public void isAlive(NodeInfo info) {
        activeNodes.put(info.getNodeId(), new NodeRecord(info, System.currentTimeMillis()));
    }

    public void checkFailures() {
        long now = System.currentTimeMillis();
        activeNodes.entrySet().removeIf(entry -> {
            boolean isDead = (now - entry.getValue().lastHeartbeat) > TIMEOUT;
            if (isDead) {
                System.out.println(entry.getKey() + " is dead");
            }
            return isDead;
        });
    }

    public Set<NodeInfo> getAliveNodes() {
        return activeNodes.values().stream()
                .map(record-> record.info)
                .collect(Collectors.toSet());
    }

    public boolean isAlive(String nodeId) {
        return activeNodes.containsKey(nodeId);
    }

    public void stop() {
        failureDetector.shutdown();
    }

}
