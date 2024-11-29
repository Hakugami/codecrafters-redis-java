package replica;

import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

import config.ObjectFactory;
import replica.ReplicaClient;

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
        String[] args = command.split(" ");

        StringBuilder formattedCommand = new StringBuilder();
        formattedCommand.append("*").append(args.length).append("\r\n");
        for (String arg : args) {
            formattedCommand.append("$").append(arg.length()).append("\r\n");
            formattedCommand.append(arg).append("\r\n");
        }

        byte[] data = formattedCommand.toString().getBytes(StandardCharsets.UTF_8);
        replicaClient.send(data);
    }
}