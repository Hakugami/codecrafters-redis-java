package storage;

import java.util.concurrent.ConcurrentHashMap;
import protocol.ValueType;

public class Storage {
    private final ConcurrentHashMap<String, StorageRecord> store = new ConcurrentHashMap<>();
    
    public void set(String key, byte[] value, ValueType type) {
        store.put(key, new StorageRecord(type, value));
    }
    
    public StorageRecord get(String key) {
        return store.get(key);
    }
}
