import java.io.Serializable;

public class Job implements Serializable {
    private final String jobId;
    private final int simulatedDurationMs;

    public Job(String jobId, int simulatedDurationMs) {
        this.jobId = jobId;
        this.simulatedDurationMs = simulatedDurationMs;
    }

    public String getJobId() {
        return jobId;
    }
    public int getSimulatedDurationMs() {
        return simulatedDurationMs;
    }

    public String toString() {
        return ("Job{" + "jobId=" + jobId + " | " + "duration=" + simulatedDurationMs + " ms}");
    }
}
