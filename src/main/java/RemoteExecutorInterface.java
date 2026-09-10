import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface RemoteExecutorInterface extends Remote {

    // Raft Consensus: Election RPC
    VoteResponse requestVote(int term, String candidateId, int lastLogIndex, int lastLogTerm) throws RemoteException;

    // Raft Consensus: Log Replication + Heartbeat RPC
    AppendEntriesResponse appendEntries(int term, String leaderId, int prevLogIndex, int prevLogTerm,
                                        List<LogEntryJob> entries, int leaderCommit) throws RemoteException;

    // Submit job to the cluster (Client -> Any Node)
    String submitJob(Job<?> job) throws RemoteException;

    // The leader calls this method on the follower/worker to execute the job
    void executeJob(Job<?> job, int attempt) throws RemoteException;

    // Backward-compatible overload for executeJob
    void executeJob(Job<?> job) throws RemoteException;

    // Worker notifies the Leader upon job completion or failure
    void notifyJobCompletion(String jobId, int attempt, Object result, boolean success, String errorMsg) throws RemoteException;

    // The client can retrieve the result (or local lookup)
    Object getJobResult(String jobId) throws RemoteException;
    Object getLocalJobResult(String jobId) throws RemoteException;

    void updateLeader(String leaderNodeId) throws RemoteException;
}
