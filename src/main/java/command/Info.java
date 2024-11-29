package command;

import config.ObjectFactory;

public class Info extends AbstractHandler {
    public Info(ObjectFactory objectFactory) {
        super(objectFactory);
    }

    @Override
    public byte[] handle(String[] args) {
        String replicationRole;
        if (ObjectFactory.getInstance().getProperties().isMaster()) {
            replicationRole = "role:master";
        }else {
            replicationRole = "role:slave";
        }
        String arg = args[1];

        return parseArgs(arg, replicationRole);
    }

    private byte[] parseArgs(String arg, String replicationRole) {
        return switch (arg) {
            case "replication" -> {
                String replicationId = ObjectFactory.getInstance().getProperties().getReplicationId();
                long replicationOffset = ObjectFactory.getInstance().getProperties().getReplicationOffset().get();
                StringBuilder bulkString = new StringBuilder();
                bulkString.append(replicationRole).append("\n");
                bulkString.append("master_replid:").append(replicationId).append("\n");
                bulkString.append("master_repl_offset:").append(replicationOffset).append("\n");
                yield protocolSerializer.bulkStrings(bulkString.toString());
            }
            case "persistence" -> "persistence:yes".getBytes();
            default -> "ERR unknown subcommand".getBytes();
        };
    }
}
