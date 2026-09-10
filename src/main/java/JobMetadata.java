import java.io.Serializable;

public class JobMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum State {
        SUBMITTED,
        ASSIGNED,
        RUNNING,
        COMPLETED,
        FAILED
    }

    private final String jobId;
    private final String clientId;
    private final long requestId;
    private final Job<?> jobObject;
    private State state;
    private String assignedNodeId;
    private int attempt;
    private Object result;
    private String errorMessage;
    private final long createdAt;
    private long lastUpdated;

    public JobMetadata(Job<?> job) {
        this.jobId = job.getJobId();
        this.clientId = job.getClientId();
        this.requestId = job.getRequestId();
        this.jobObject = job;
        this.state = State.SUBMITTED;
        this.assignedNodeId = null;
        this.attempt = job.getAttempt() > 0 ? job.getAttempt() : 1;
        this.result = null;
        this.errorMessage = null;
        this.createdAt = System.currentTimeMillis();
        this.lastUpdated = this.createdAt;
    }

    public String getJobId() {
        return jobId;
    }

    public String getClientId() {
        return clientId;
    }

    public long getRequestId() {
        return requestId;
    }

    public Job<?> getJobObject() {
        return jobObject;
    }

    public synchronized State getState() {
        return state;
    }

    public synchronized void setState(State state) {
        this.state = state;
        this.lastUpdated = System.currentTimeMillis();
    }

    public synchronized String getAssignedNodeId() {
        return assignedNodeId;
    }

    public synchronized void setAssignedNodeId(String assignedNodeId) {
        this.assignedNodeId = assignedNodeId;
        this.lastUpdated = System.currentTimeMillis();
    }

    public synchronized int getAttempt() {
        return attempt;
    }

    public synchronized void setAttempt(int attempt) {
        this.attempt = attempt;
        this.lastUpdated = System.currentTimeMillis();
    }

    public synchronized Object getResult() {
        return result;
    }

    public synchronized void setResult(Object result) {
        this.result = result;
        this.lastUpdated = System.currentTimeMillis();
    }

    public synchronized String getErrorMessage() {
        return errorMessage;
    }

    public synchronized void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
        this.lastUpdated = System.currentTimeMillis();
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public synchronized long getLastUpdated() {
        return lastUpdated;
    }

    @Override
    public synchronized String toString() {
        return "JobMetadata{" +
                "jobId='" + jobId + '\'' +
                ", clientId='" + clientId + '\'' +
                ", requestId=" + requestId +
                ", state=" + state +
                ", assignedNodeId='" + assignedNodeId + '\'' +
                ", attempt=" + attempt +
                ", result=" + result +
                ", errorMessage='" + errorMessage + '\'' +
                '}';
    }
}
