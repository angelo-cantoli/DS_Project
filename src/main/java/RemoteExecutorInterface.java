import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface RemoteExecutorInterface extends Remote {

    VoteResponse requestVote(int term, String candidateId, int lastLogIndex, int lastLogTerm) throws RemoteException;

    AppendEntriesResponse appendEntries(int term, String leaderId, int prevLogIndex, int prevLogTerm,
                                        List<LogEntryJob> entries, int leaderCommit) throws RemoteException;

    String submitJob(Job<?> job) throws RemoteException;

    void executeJob(Job<?> job, int attempt) throws RemoteException;

    void notifyJobCompletion(String jobId, int attempt, Object result, boolean success, String errorMsg) throws RemoteException;

    Object getJobResult(String jobId) throws RemoteException;
}
