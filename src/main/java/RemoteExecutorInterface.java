import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteExecutorInterface extends Remote {
    String submitJob(Job<?> job) throws RemoteException;
    void executeJob(Job<?> job) throws RemoteException;
    Object getJobResult(String jobId) throws RemoteException;
    void updateLeader(String leaderNodeId) throws RemoteException;
    
    // Raft Consensus RPC
    boolean requestVote(int term, String candidateId) throws RemoteException;
}
