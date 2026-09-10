import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.List;

public class LogViewer {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: java LogViewer <nodeId oppure nome_file.dat>");
            System.out.println("Esempi:");
            System.out.println("  ./view-log.sh node-A");
            System.out.println("  ./view-log.sh node-A_raft_log.dat");
            return;
        }

        String filename = args[0];
        if (!filename.endsWith(".dat")) {
            filename = filename + "_raft_log.dat";
        }

        File file = new File(filename);
        if (!file.exists()) {
            System.err.println("File non trovato: " + file.getAbsolutePath());
            return;
        }

        System.out.println("\n=========================================================================================================");
        System.out.println("                      CONTENUTO DEL RAFT LOG: " + file.getName());
        System.out.println("=========================================================================================================");
        System.out.printf("%-7s | %-6s | %-15s | %-36s | %s%n", "INDEX", "TERM", "COMMAND TYPE", "JOB ID", "PAYLOAD / DETTAGLI");
        System.out.println("---------------------------------------------------------------------------------------------------------");

        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(file))) {
            List<LogEntryJob> entries = (List<LogEntryJob>) in.readObject();

            if (entries == null || entries.isEmpty()) {
                System.out.println("Il log è completamente vuoto.");
                return;
            }

            for (int i = 0; i < entries.size(); i++) {
                LogEntryJob entry = entries.get(i);
                if (i == 0) {
                    System.out.printf("%-7d | %-6d | %-15s | %-36s | %s%n",
                            entry.getIndex(), entry.getTerm(), "[SENTINELLA]", "-", "(Indice 0 sentinella Raft)");
                } else {
                    String payloadStr = formatPayload(entry.getPayload());
                    System.out.printf("%-7d | %-6d | %-15s | %-36s | %s%n",
                            entry.getIndex(),
                            entry.getTerm(),
                            entry.getType(),
                            entry.getJobId() != null ? entry.getJobId() : "-",
                            payloadStr);
                }
            }
            System.out.println("=========================================================================================================");
            System.out.println("Totale entry registrate (esclusa sentinella): " + (entries.size() - 1) + "\n");

        } catch (Exception e) {
            System.err.println("Errore durante la lettura del file di log: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static String formatPayload(Object payload) {
        if (payload == null) {
            return "null";
        }
        if (payload instanceof Object[]) {
            return Arrays.toString((Object[]) payload);
        }
        if (payload instanceof Job<?>) {
            Job<?> job = (Job<?>) payload;
            return "Job[Client=" + job.getClientId() + ", ReqId=" + job.getRequestId() + ", Attempt=" + job.getAttempt() + "]";
        }
        return payload.toString();
    }
}
