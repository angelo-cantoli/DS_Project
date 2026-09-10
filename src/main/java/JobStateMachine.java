import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JobStateMachine {
    private final Map<String, JobMetadata> jobs = new ConcurrentHashMap<>();
    // Mappa per la deduplicazione: clientId -> (requestId -> jobId)
    private final Map<String, Map<Long, String>> clientRequests = new ConcurrentHashMap<>();
    private int lastAppliedIndex = 0;

    public synchronized void apply(LogEntryJob entry) {
        if (entry == null || entry.getType() == null) {
            return;
        }

        switch (entry.getType()) {
            case CREATE_JOB:
                if (entry.getPayload() instanceof Job<?>) {
                    Job<?> job = (Job<?>) entry.getPayload();
                    JobMetadata meta = new JobMetadata(job);
                    jobs.put(entry.getJobId(), meta);

                    // Registra nella mappa di deduplicazione se sono presenti clientId e requestId
                    if (job.getClientId() != null && job.getRequestId() > 0) {
                        clientRequests.computeIfAbsent(job.getClientId(), k -> new ConcurrentHashMap<>())
                                      .put(job.getRequestId(), entry.getJobId());
                    }
                }
                break;

            case ASSIGN_JOB:
                JobMetadata assignMeta = jobs.get(entry.getJobId());
                if (assignMeta != null) {
                    if (entry.getPayload() instanceof String) {
                        assignMeta.setAssignedNodeId((String) entry.getPayload());
                    } else if (entry.getPayload() instanceof Object[]) {
                        Object[] arr = (Object[]) entry.getPayload();
                        if (arr.length > 0 && arr[0] instanceof String) {
                            assignMeta.setAssignedNodeId((String) arr[0]);
                        }
                        if (arr.length > 1 && arr[1] instanceof Integer) {
                            assignMeta.setAttempt((Integer) arr[1]);
                        }
                    }
                    assignMeta.setState(JobMetadata.State.ASSIGNED);
                }
                break;

            case START_JOB:
                JobMetadata startMeta = jobs.get(entry.getJobId());
                if (startMeta != null) {
                    startMeta.setState(JobMetadata.State.RUNNING);
                }
                break;

            case COMPLETE_JOB:
                JobMetadata comp = jobs.get(entry.getJobId());
                if (comp != null) {
                    comp.setResult(entry.getPayload());
                    comp.setState(JobMetadata.State.COMPLETED);
                }
                break;

            case FAIL_JOB:
                JobMetadata failMeta = jobs.get(entry.getJobId());
                if (failMeta != null) {
                    failMeta.setErrorMessage(entry.getPayload() != null ? entry.getPayload().toString() : "Unknown failure");
                    failMeta.setState(JobMetadata.State.FAILED);
                }
                break;

            case REQUEUE_JOB:
                JobMetadata req = jobs.get(entry.getJobId());
                if (req != null) {
                    req.setAssignedNodeId(null);
                    req.setState(JobMetadata.State.SUBMITTED);
                    req.setAttempt(req.getAttempt() + 1);
                }
                break;
        }

        lastAppliedIndex = entry.getIndex();
    }

    public String getExistingJobId(String clientId, long requestId) {
        if (clientId == null || requestId <= 0) return null;
        Map<Long, String> reqMap = clientRequests.get(clientId);
        return reqMap != null ? reqMap.get(requestId) : null;
    }

    public JobMetadata getJob(String jobId) {
        return jobs.get(jobId);
    }

    public Map<String, JobMetadata> getAllJobs() {
        return Collections.unmodifiableMap(jobs);
    }

    public synchronized int getLastAppliedIndex() {
        return lastAppliedIndex;
    }
}