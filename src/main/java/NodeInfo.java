import java.io.Serializable;
import java.util.Objects;

public class NodeInfo implements Serializable {
    private final String nodeId;
    private final String ipAddress;
    private final int port;
    private final int activeJobs;
    private final int term;
    private final boolean isLeader;

    public NodeInfo(String nodeId, String ipAddress, int port, int activeJobs, int term, boolean isLeader) {
        this.nodeId = nodeId;
        this.ipAddress = ipAddress;
        this.port = port;
        this.activeJobs = activeJobs;
        this.term = term;
        this.isLeader = isLeader;
    }

    public String getNodeId() { return nodeId; }
    public String getIpAddress() { return ipAddress; }
    public int getPort() { return port; }
    public int getActiveJobs() { return activeJobs; }
    public int getTerm() { return term; }
    public boolean isLeader() { return isLeader; }

    //define when two nodes are equals or not to avoid duplicates on the cluster
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeInfo nodeInfo = (NodeInfo) o;
        return Objects.equals(nodeId, nodeInfo.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId);
    }
}
