package config;

import command.factory.CommandFactory;

import protocol.ProtocolDeserializer;
import protocol.ProtocolSerializer;
import protocol.persistence.PersistenceManager;
import storage.StorageRecord;

import java.io.IOException;
import java.util.Map;


public class ObjectFactory {
    private ApplicationProperties properties;
    private CommandFactory commandFactory;
    private ProtocolSerializer protocolSerializer;
    private ProtocolDeserializer protocolDeserializer;
    private PersistenceManager persistenceManager;

    private ObjectFactory() {
    }

    public static ObjectFactory getInstance(String[] args) throws IOException {
        if (ObjectFactoryHolder.INSTANCE.properties == null) {
            ObjectFactoryHolder.INSTANCE.init(args);
            Map<String, StorageRecord> stringStorageRecordMap = ObjectFactoryHolder.INSTANCE.persistenceManager.getRdbLoader().readAllPairs();
            ObjectFactoryHolder.INSTANCE.persistenceManager.getStorage().setStore(stringStorageRecordMap);
        }
        return ObjectFactoryHolder.INSTANCE;
    }

    public static ObjectFactory getInstance() {
        return ObjectFactoryHolder.INSTANCE;
    }

    private static class ObjectFactoryHolder {
        private static final ObjectFactory INSTANCE = new ObjectFactory();
    }

    public void init(String[] args) {
        this.properties = new ApplicationProperties(args);
        this.persistenceManager = new PersistenceManager();
        this.protocolSerializer = new ProtocolSerializer();
        this.protocolDeserializer = new ProtocolDeserializer();
        this.commandFactory = new CommandFactory(ObjectFactoryHolder.INSTANCE);
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

    public PersistenceManager getPersistenceManager() {
        return persistenceManager;
    }
}
