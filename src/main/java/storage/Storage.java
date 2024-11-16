package storage;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;

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

    public Map<String, StorageRecord> getStore() {
        Map<String, StorageRecord> copy = new ConcurrentHashMap<>();
        store.forEach((k, v) -> copy.put(k, new StorageRecord(v.type(), v.data(), v.expiry())));
        return copy;
    }

    public Set<String> getAllKeys() {
        return store.keySet();
    }

    public void setStore(Map<String, StorageRecord> store) {
        this.store.clear();
        store.forEach((k, v) -> this.store.put(k, new StorageRecord(v.type(), v.data(), v.expiry())));
    }
}
