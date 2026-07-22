import java.io.Serializable;

public class Job<T> implements Serializable {
    private final String jobId;
    private final SerializableSupplier<T> task;

    public Job(String jobId, SerializableSupplier<T> task) {
        this.jobId = jobId;
        this.task = task;
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
