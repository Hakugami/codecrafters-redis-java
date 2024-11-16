package config;


import java.security.SecureRandom;

public class ApplicationProperties {
    private int port = 6379;

    // RDB file properties
    private String dir = System.getProperty("user.dir");
    private String dbFileName = "dump.rdb";

    public ApplicationProperties(String[] args) {
        parseArgs(args);
    }

    private void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String param = args[i].toLowerCase().substring(2);
            switch (param) {
                case "port" -> port = Integer.parseInt(args[++i]);
                case "dir" -> dir = args[++i];
                case "dbfilename" -> dbFileName = args[++i];
                default -> throw new IllegalArgumentException("Invalid argument: " + param);
            }
        }
    }

    public static String generateRandomString(int length) {
        SecureRandom random = new SecureRandom();
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) (random.nextInt(26) + 'a'));
        }
        return sb.toString();
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public String getDbFileName() {
        return dbFileName;
    }

    public void setDbFileName(String dbFileName) {
        this.dbFileName = dbFileName;
    }
}
