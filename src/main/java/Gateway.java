import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.atomic.AtomicReference;

public class Gateway extends UnicastRemoteObject implements RemoteExecutorInterface {
    
    // The Gateway only cares about who the current Leader is.
    private final AtomicReference<NodeInfo> currentLeader = new AtomicReference<>(null);
    private int highestTerm = -1;

    public Gateway() throws RemoteException {
        super();
        startUDPListener();
    }

    private void startUDPListener() {
        new Thread(() -> {
            try (MulticastSocket socket = new MulticastSocket(4446)) {
                // Removed 127.0.0.1 hardcode to allow binding to physical Wi-Fi/Ethernet cards
                InetAddress group = InetAddress.getByName("230.0.0.0");
                socket.joinGroup(group);
                byte[] buffer = new byte[256];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                System.out.println("Gateway listening for cluster heartbeats...");
                while (true) {
                    socket.receive(packet);
                    String[] parts = new String(packet.getData(), 0, packet.getLength()).split(",");
                    if (parts.length >= 6) {
                        String senderId = parts[0];
                        String senderIp = parts[1];
                        int senderPort = Integer.parseInt(parts[2]);
                        int activeJobs = Integer.parseInt(parts[3]);
                        int term = Integer.parseInt(parts[4]);
                        boolean isLeader = Boolean.parseBoolean(parts[5]);

                        // Passively observe the network to find the Leader
                        if (isLeader && term >= highestTerm) {
                            highestTerm = term;
                            NodeInfo leaderInfo = new NodeInfo(senderId, senderIp, senderPort, activeJobs, term, isLeader);
                            NodeInfo oldLeader = currentLeader.getAndSet(leaderInfo);
                            
                            if (oldLeader == null || !oldLeader.getNodeId().equals(senderId)) {
                                System.out.println("[Gateway] Detected active LEADER: " + senderId + " (Term: " + term + ")");
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    private RemoteExecutorInterface getLeaderStub() throws Exception {
        NodeInfo leader = currentLeader.get();
        if (leader == null) {
            throw new RemoteException("No leader currently available in the cluster. Waiting for election...");
        }
        Registry registry = LocateRegistry.getRegistry(leader.getIpAddress(), leader.getPort());
        return (RemoteExecutorInterface) registry.lookup("Executor");
    }

    @Override
    public String submitJob(Job<?> job) throws RemoteException {
        // Robust Retry Logic hidden from the Client!
        int retries = 5; 
        while (retries > 0) {
            try {
                System.out.println("[Gateway] Forwarding Job " + job.getJobId() + " to Leader...");
                return getLeaderStub().submitJob(job);
            } catch (Exception e) {
                System.out.println("[Gateway] Failed to reach leader, retrying... (" + retries + " attempts left)");
                retries--;
                currentLeader.set(null); // Wipe the dead leader so the UDP listener can find the new one
                try { Thread.sleep(3000); } catch (InterruptedException ignored) {}
            }
        }
        throw new RemoteException("Gateway failed to submit job after 5 retries. Cluster might be entirely offline.");
    }

    @Override
    public Object getJobResult(String jobId) throws RemoteException {
        try {
            return getLeaderStub().getJobResult(jobId);
        } catch (Exception e) {
            // If polling fails, wipe the leader. The client will poll again in 1 second anyway.
            currentLeader.set(null); 
            return null; 
        }
    }

    // --- Unused methods for the Gateway ---
    @Override
    public void executeJob(Job<?> job) throws RemoteException {
        throw new RemoteException("Gateway does not execute jobs.");
    }

    @Override
    public void updateLeader(String leaderNodeId) throws RemoteException {}

    @Override
    public boolean requestVote(int term, String candidateId) throws RemoteException {
        return false; // Gateway does not participate in elections
    }

    public static void main(String[] args) {
        try {
            Gateway gateway = new Gateway();
            // Start the Gateway's RMI Registry on port 8000
            Registry registry = LocateRegistry.createRegistry(8000);
            registry.rebind("Executor", gateway);
            String realIp = java.net.InetAddress.getLocalHost().getHostAddress();
            System.out.println("==================================================");
            System.out.println("=== API Gateway / Load Balancer running on 8000 ==");
            System.out.println("=== Gateway IP Address: " + realIp);
            System.out.println("==================================================");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
