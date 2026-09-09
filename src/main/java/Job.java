import java.io.Serializable;

public class Job<T> implements Serializable {
    private String jobId;
    private final SerializableSupplier<T> task;

    public Job(SerializableSupplier<T> task) {
        this.jobId = null;
        this.task = task;
    }

    public Job(String jobId, SerializableSupplier<T> task) {
        this.jobId = jobId;
        this.task = task;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }

    public T execute() {
        return task.get();
    }

    @Override
    public String toString() {
        return "Job{jobId='" + jobId + "'}";
    }
}
