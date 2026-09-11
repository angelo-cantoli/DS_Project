import java.io.Serializable;

public class Job<T> implements Serializable {
    private static final long serialVersionUID = 1L;

    private String jobId;
    private String clientId;
    private long requestId;
    private int attempt = 1;
    private final SerializableSupplier<T> task;

    public Job(SerializableSupplier<T> task) {
        this(null, null, 0L, 1, task);
    }

    public Job(String clientId, long requestId, SerializableSupplier<T> task) {
        this(null, clientId, requestId, 1, task);
    }

    public Job(String jobId, SerializableSupplier<T> task) {
        this(jobId, null, 0L, 1, task);
    }

    public Job(String jobId, String clientId, long requestId, int attempt, SerializableSupplier<T> task) {
        this.jobId = jobId;
        this.clientId = clientId;
        this.requestId = requestId;
        this.attempt = attempt > 0 ? attempt : 1;
        this.task = task;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }

    public String getClientId() {
        return clientId;
    }

     void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public long getRequestId() {
        return requestId;
    }

    public void setRequestId(long requestId) {
        this.requestId = requestId;
    }

    public int getAttempt() {
        return attempt;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    public T execute() {
        return task != null ? task.get() : null;
    }

    @Override
    public String toString() {
        return "Job{" +
                "jobId='" + jobId + '\'' +
                ", clientId='" + clientId + '\'' +
                ", requestId=" + requestId +
                ", attempt=" + attempt +
                '}';
    }
}
