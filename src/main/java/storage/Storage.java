package storage;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import protocol.ValueType;

public class Storage {
    private final Map<String, StorageRecord> store;

    public Storage() {
        this.store = new ConcurrentHashMap<>();
    }

    public void set(String key, byte[] value, ValueType type, Instant expiry) {
        store.put(key, new StorageRecord(type, value, expiry));
    }

    public StorageRecord get(String key) {
        StorageRecord record = store.get(key);
        if (record != null && record.expiry() != null) {
            if (Instant.now().isAfter(record.expiry())) {
                store.remove(key);
                return null;
            }
        }
        return record;
    }
}
