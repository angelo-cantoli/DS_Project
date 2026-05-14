import java.rmi.RemoteException;
import java.util.concurrent.Executor;

public interface RemoteExecutorInterface extends Executor {

    String submitJob(Job job) throws RemoteException;

    String getJobResult(String jobId) throws RemoteException;

    void updateLeader(String leaderNodeId) throws RemoteException;
}
