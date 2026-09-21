import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class Config {
    private static final String CONFIG_FILE = "cluster.properties";
    
    public static String getMyIp() {
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(CONFIG_FILE)) {
            props.load(in);
            return props.getProperty("my.ip", "127.0.0.1").trim();
        } catch (IOException e) {
            System.err.println("[ATTENZIONE] File " + CONFIG_FILE + " non trovato. Uso IP di default: 127.0.0.1");
            return "127.0.0.1";
        }
    }
    
    public static List<String> getClusterIps() {
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(CONFIG_FILE)) {
            props.load(in);
            String ips = props.getProperty("cluster.ips", "127.0.0.1");
            return Arrays.stream(ips.split(","))
                         .map(String::trim)
                         .collect(Collectors.toList());
        } catch (IOException e) {
            return Arrays.asList("127.0.0.1");
        }
    }
}
