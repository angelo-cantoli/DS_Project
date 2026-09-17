import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RaftLog {
    private final String nodeId;
    private final File logFile;
    private final List<LogEntryJob> entries;
    private int commitIndex = 0;
    private int lastApplied = 0;

    public RaftLog(String nodeId) {
        this.nodeId = nodeId;
        //Il formato binario .dat con ObjectOutputStream serializza nativamente qualsiasi grafo di oggetti Java,
        // compresi il bytecode delle lambda e i tipi generici, preservando l'integrità del calcolo al 100%.
        this.logFile = new File(nodeId + "_raft_log.dat");
        this.entries = new ArrayList<>();
        this.entries.add(new LogEntryJob(0, 0, null, null, null));
        loadFromDisk();
    }

    public synchronized int getLastLogIndex() {
        return entries.size() - 1;
    }

    public synchronized int getLastLogTerm() {
        return entries.get(entries.size() - 1).getTerm();
    }

    public synchronized LogEntryJob getEntry(int index) {
        if (index >= 0 && index < entries.size()) {
            return entries.get(index);
        }
        return null;
    }

    public synchronized int append(int term, LogEntryJob.Type type, String jobId, Object payload) {
        int newIndex = entries.size();
        LogEntryJob entry = new LogEntryJob(term, newIndex, type, jobId, payload);
        entries.add(entry);
        saveToDisk();
        System.out.println("[" + nodeId + " RaftLog] Appended new entry: index=" + newIndex + ", term=" + term + ", type=" + type + ", jobId=" + jobId);
        return newIndex;
    }

    public synchronized void append(LogEntryJob entry) {
        entries.add(entry);
        saveToDisk();
        System.out.println("[" + nodeId + " RaftLog] Appended replicated entry: index=" + entry.getIndex() + ", term=" + entry.getTerm() + ", type=" + entry.getType());
    }

    public synchronized void truncate(int fromIndex) {
        if (fromIndex > 0 && fromIndex < entries.size()) {
            System.out.println("[" + nodeId + " RaftLog] Truncating log from index " + fromIndex + " to " + (entries.size() - 1));
            while (entries.size() > fromIndex) {
                entries.remove(entries.size() - 1);
            }
            saveToDisk();
        }
    }

    public synchronized List<LogEntryJob> getEntriesFrom(int fromIndex) {
        if (fromIndex <= 0 || fromIndex >= entries.size()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(entries.subList(fromIndex, entries.size()));
    }

    public synchronized boolean isUpToDate(int candidateLastLogIndex, int candidateLastLogTerm) {
        int myLastLogTerm = getLastLogTerm();
        int myLastLogIndex = getLastLogIndex();
        if (candidateLastLogTerm != myLastLogTerm) {
            return candidateLastLogTerm > myLastLogTerm;
        }
        return candidateLastLogIndex >= myLastLogIndex;
    }

    public synchronized int getCommitIndex() {
        return commitIndex;
    }

    public synchronized void setCommitIndex(int commitIndex) {
        if (commitIndex > this.commitIndex) {
            this.commitIndex = commitIndex;
        }
    }

    public synchronized int getLastApplied() {
        return lastApplied;
    }

    public synchronized void setLastApplied(int lastApplied) {
        this.lastApplied = lastApplied;
    }

    @SuppressWarnings("unchecked")
    private synchronized void loadFromDisk() {
        if (!logFile.exists()) {
            System.out.println("[" + nodeId + " RaftLog] Nessun log precedente trovato. Inizializzato nuovo log vuoto.");
            return;
        }

        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(logFile))) {
            List<LogEntryJob> loaded = (List<LogEntryJob>) in.readObject();
            if (loaded != null && !loaded.isEmpty()) {
                entries.clear();
                entries.addAll(loaded);
                System.out.println("[" + nodeId + " RaftLog] Ripristinato log da disco: " + (entries.size() - 1) + " entries caricate. LastLogIndex=" + getLastLogIndex() + ", LastLogTerm=" + getLastLogTerm());
            }
        } catch (Exception e) {
            System.err.println("[" + nodeId + " RaftLog] Errore durante il caricamento del log da disco: " + e.getMessage());
        }
    }

    private synchronized void saveToDisk() {
        File tempFile = new File(nodeId + "_raft_log.dat.tmp");
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(tempFile))) {
            out.writeObject(new ArrayList<>(entries));
            out.flush();
        } catch (IOException e) {
            System.err.println("[" + nodeId + " RaftLog] Errore durante il salvataggio del log: " + e.getMessage());
            return;
        }

        if (tempFile.exists()) {
            if (logFile.exists()) {
                logFile.delete();
            }
            //viene usata questa soluzione perchè nei OS moderni in caso di crash non sporcherà mai il file con qualcosa di obsoleto
            //la rinomina di un file è un'operazione atomica a livello di filesystem:
            // se salta la corrente o crasha la JVM durante la scrittura,
            // il vecchio log .dat non risulterà mai corrotto o troncato a metà!
            tempFile.renameTo(logFile);
        }
    }
}
