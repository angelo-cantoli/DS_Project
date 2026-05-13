import java.util.Set;

public interface ClusterStatus {

    Set<NodeInfo> getAliveNodes();

    boolean isAlive(String nodeId);
}
