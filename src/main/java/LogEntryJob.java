import java.io.Serializable;

public class LogEntryJob implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum Type {
        CREATE_JOB,
        ASSIGN_JOB,
        START_JOB,
        COMPLETE_JOB,
        FAIL_JOB,
        REQUEUE_JOB
    }

    private final int term;
    private final int index;
    private final Type type;
    private final String jobId;
    private final Object payload; // Payload leggero (ID del nodo, metadati, attempt, ecc.)

    public LogEntryJob(int term, int index, Type type, String jobId, Object payload) {
        this.term = term;
        this.index = index;
        this.type = type;
        this.jobId = jobId;
        this.payload = payload;
    }

    public int getTerm() {
        return term;
    }

    public int getIndex() {
        return index;
    }

    public Type getType() {
        return type;
    }

    public String getJobId() {
        return jobId;
    }

    public Object getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "LogEntryJob{" +
                "term=" + term +
                ", index=" + index +
                ", type=" + type +
                ", jobId='" + jobId + '\'' +
                ", payload=" + payload +
                '}';
    }
}
