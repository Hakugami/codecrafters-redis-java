package config;

import command.CommandFactory;
import lombok.Data;
import protocol.ProtocolDeserializer;
import protocol.ProtocolSerializer;


public class ObjectFactory {
    private final ApplicationProperties properties;
    private final CommandFactory commandFactory;
    private final ProtocolSerializer protocolSerializer ;
    private final ProtocolDeserializer protocolDeserializer;

    public ObjectFactory(ApplicationProperties properties) {
        this.properties = properties;
        this.protocolSerializer = new ProtocolSerializer();
        this.protocolDeserializer = new ProtocolDeserializer();
        this.commandFactory = new CommandFactory(this);
    }

    public ApplicationProperties getProperties() {
        return properties;
    }

    public CommandFactory getCommandFactory() {
        return commandFactory;
    }

    public ProtocolSerializer getProtocolSerializer() {
        return protocolSerializer;
    }

    public ProtocolDeserializer getProtocolDeserializer() {
        return protocolDeserializer;
    }
}
