package command;

import config.ObjectFactory;
import protocol.persistence.PersistenceManager;

public class Save extends AbstractHandler {
    private final PersistenceManager persistenceManager;
    public Save(ObjectFactory objectFactory) {
        super(objectFactory);
        this.persistenceManager = objectFactory.getPersistenceManager();
    }

    @Override
    public byte[] handle(String[] args) {
        persistenceManager.getRdbProcessor().saveAllKeys();
        return protocolSerializer.simpleString("OK");
    }
}
