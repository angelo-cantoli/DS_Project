import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Executor extends UnicastRemoteObject implements RemoteExecutorInterface {
    private final int nodeId;
    private ConcurrentMap jobResults;
    private int currentLeader;


    public Executor(int nodeId) throws RemoteException {
        this.nodeId = nodeId;
        this.currentLeader = 0;
        this.jobResults = new ConcurrentHashMap();
    }

    private boolean isLeader(){
        return currentLeader == nodeId;
    }

    public int submitJob(Job job) throws RemoteException {
        System.out.println("Submitting job: " + job.getJobId());

        if(isLeader()){
            // Loadbalancing
        } else {
            // Mando al leader
        }
        return job.getJobId();
    }

    @Override
    public void executeJob(Job job) throws RemoteException {
        System.out.println("Executing job: " + job.getJobId());
        // Bho magari aggiungiamo un timeout per simulare un job
        int result = 0; // Questo sarà un numero per simulare l'output del job eseguito
        jobResults.put(job.getJobId(), result);
    }

    @Override
    public int getJobResult(String jobId) throws RemoteException {
        return this.jobResults.get(jobId).toString();
    }

    @Override
    public void updateLeader(int leaderNodeId) throws RemoteException {
        this.currentLeader = leaderNodeId;
    }
}
