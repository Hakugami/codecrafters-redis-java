package replica;

import java.util.logging.Logger;

import config.ObjectFactory;

public class CommandReplicator {
    private static final Logger logger = Logger.getLogger(CommandReplicator.class.getName());

    public void replicateWriteCommand(String command) {
        var replicaClients = ObjectFactory.getInstance().getProperties().getReplicaClients();
        if (replicaClients == null || replicaClients.isEmpty()) {
            logger.info("No replica clients available for replication");
            return;
        }

        logger.info("Replicating command to " + replicaClients.size() + " replicas: " + command);
        replicaClients.forEach(replicaClient -> replicate(replicaClient, command));
    }

    private void replicate(ReplicaClient replicaClient, String command) {
        StringBuilder formatted = new StringBuilder();
        String[] args = command.split(" ");
        formatted.append("*").append(args.length).append("\r\n");

        for (String arg : args) {
            formatted.append("$").append(arg.length()).append("\r\n");
            formatted.append(arg).append("\r\n");
        }

        logger.info("Sending formatted command to replica: " + formatted);
        replicaClient.send(formatted.toString().getBytes());
    }
}