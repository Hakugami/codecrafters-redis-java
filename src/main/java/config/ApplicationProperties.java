package config;


import java.security.SecureRandom;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import replica.ReplicaClient;

public class ApplicationProperties {
    private int port = 6379;

    //Replication
    private ReplicaProperties replicaProperties;

    //Replication Master
    private String replicationId;
    private AtomicLong replicationOffset = new AtomicLong(0);
    private List<ReplicaClient> replicaClients;


    // RDB file properties
    private String dir = System.getProperty("user.dir");
    private String dbFileName = "dump.rdb";

    public ApplicationProperties(String[] args) {
        parseArgs(args);
        if (isMaster()) {
            setMasterProperties();
        }
    }

    public void addReplicaClient(ReplicaClient replicaClient) {
        if (replicaClients == null) {
            replicaClients = new CopyOnWriteArrayList<>();
        }
        replicaClients.add(replicaClient);
    }

    private void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String param = args[i].toLowerCase().substring(2);
            switch (param) {
                case "port" -> port = Integer.parseInt(args[++i]);
                case "dir" -> dir = args[++i];
                case "dbfilename" -> dbFileName = args[++i];
                case "replicaof" -> {
                    String[] replicaParams = args[++i].split(" ");
                    replicaProperties = new ReplicaProperties(replicaParams[0], Integer.parseInt(replicaParams[1]));
                }
                default -> throw new IllegalArgumentException("Invalid argument: " + param);
            }
        }
    }

    private void setMasterProperties() {
        this.replicationId = generateRandomString(40);
        this.replicationOffset = new AtomicLong(0);
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

    public ReplicaProperties getReplicaProperties() {
        return replicaProperties;
    }

    public String getReplicationId() {
        return replicationId;
    }

    public AtomicLong getReplicationOffset() {
        return replicationOffset;
    }

    public List<ReplicaClient> getReplicaClients() {
        return replicaClients;
    }

    public boolean isReplica() {
        return replicaProperties != null;
    }

    public boolean isMaster() {
        return replicaProperties == null;
    }
}
