import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteExecutorInterface extends Remote {

    Object getLocalJobResult(String jobId) throws RemoteException;
    //submit job to the cluster
    String submitJob(Job<?> job) throws RemoteException;
    //the leader call this method to the follower to execute the job
    void executeJob(Job<?> job) throws RemoteException;
    //the client can retrieve the result
    Object getJobResult(String jobId) throws RemoteException;
    void updateLeader(String leaderNodeId) throws RemoteException;
    
    // Raft Consensus RPC
    boolean requestVote(int term, String candidateId) throws RemoteException;
}
