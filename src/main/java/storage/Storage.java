package storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import protocol.ValueType;

public class Storage {
    private final Map<String, StorageRecord> store;

    public Storage() {
        this.store = new ConcurrentHashMap<>();
    }

    public void set(String key, byte[] value, ValueType type) {
        store.put(key, new StorageRecord(type, value));
    }

    public StorageRecord get(String key) {
        return store.get(key);
    }
}
