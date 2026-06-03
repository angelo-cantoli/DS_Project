import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.concurrent.Executor;

public interface RemoteExecutorInterface extends Remote {

    // Usato dal client per mandare il job. Se l'executor è il leader fa loadbalancing, se è un follower manda al leader
    int submitJob(Job job) throws RemoteException;

    // Chiamato dal leader per far eseguire il task ad un determinato executor
    void executeJob(Job job) throws RemoteException;

    // Chiamato dal client per conoscere lo stato di esecuzione del task. Se leader risponde, se follower inoltra la richiesta a leader
    int getJobResult(String jobId) throws RemoteException;

    // Potrebbe servire in futuro per leader election
    void updateLeader(int leaderNodeId) throws RemoteException;
}
