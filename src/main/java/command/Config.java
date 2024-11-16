package command;

import config.ApplicationProperties;
import config.ObjectFactory;

public class Config extends AbstractHandler {
    private final ApplicationProperties properties;

    public Config(ObjectFactory objectFactory) {
        super(objectFactory);
        this.properties = objectFactory.getProperties();
    }

    @Override
    public byte[] handle(String[] args) {
        logger.info("Handling CONFIG command : " + String.join(" ", args));
        String subCommand = args[1].toUpperCase();


        return switch (subCommand) {
            case "GET" -> handleGet(args);
            case "SET" -> handleSet(args);
            default ->
                    protocolSerializer.simpleError("ERR unknown subcommand or wrong number of arguments for 'config' command");
        };
    }

    private byte[] handleSet(String[] args) {
        if (args.length != 4) {
            return protocolSerializer.simpleError("ERR wrong number of arguments for 'config set' command");
        }

        String key = args[2];
        String value = args[3];

        switch (key) {
            case "dir" -> {
                properties.setDir(value);
                return protocolSerializer.simpleString("OK");
            }
            case "dbfilename" -> {
                properties.setDbFileName(value);
                return protocolSerializer.simpleString("OK");
            }
            default -> {
                return protocolSerializer.simpleError("ERR unsupported CONFIG parameter: " + key);
            }
        }
    }

    private byte[] handleGet(String[] args) {
        if (args.length != 3) {
            return protocolSerializer.simpleError("ERR wrong number of arguments for 'config get' command");
        }

        String key = args[2];
        switch (key) {
            case "dir" -> {
                return protocolSerializer.array("dir".getBytes(),properties.getDir().getBytes());
            }
            case "dbfilename" -> {
                return protocolSerializer.array("dbfilename".getBytes(), properties.getDbFileName().getBytes());
            }
            default -> {
                return protocolSerializer.nullBulkString();
            }
        }

    }
}
