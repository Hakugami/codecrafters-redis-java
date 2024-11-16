package config;

import command.factory.CommandFactory;
import protocol.ProtocolDeserializer;
import protocol.ProtocolSerializer;
import storage.Storage;


public class ObjectFactory {
    private final ApplicationProperties properties;
    private final CommandFactory commandFactory;
    private final ProtocolSerializer protocolSerializer ;
    private final ProtocolDeserializer protocolDeserializer;
    private final Storage storage;

    public ObjectFactory(ApplicationProperties properties) {
        this.properties = properties;
        this.protocolSerializer = new ProtocolSerializer();
        this.protocolDeserializer = new ProtocolDeserializer();
        this.storage = new Storage();
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

    public Storage getStorage() {
        return storage;
    }
}
