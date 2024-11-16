package protocol.persistence;

import config.ObjectFactory;
import storage.Storage;

public class PersistenceManager {
    private final RDBProcessor rdbProcessor;
    private final RDBLoader rdbLoader;
    private final Storage storage;

    public PersistenceManager() {
        this.storage = new Storage();
        this.rdbProcessor = new RDBProcessor();
        this.rdbLoader = new RDBLoader();
    }

    public RDBProcessor getRdbProcessor() {
        return rdbProcessor;
    }

    public RDBLoader getRdbLoader() {
        return rdbLoader;
    }

    public Storage getStorage() {
        return storage;
    }
}
