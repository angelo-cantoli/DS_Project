import java.io.Serializable;

public class NodeInfo implements Serializable {

    private final String nodeId;
    private final String ipAddress;
    private final int port;

    public NodeInfo(String nodeId, String ipAddress, int port) {
        this.nodeId = nodeId;
        this.ipAddress = ipAddress;
        this.port = port;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public int getPort() {
        return port;
    }
}
