package storage;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import java.util.logging.Logger;

import protocol.ValueType;

public class Storage {
    private static final Logger logger = Logger.getLogger(Storage.class.getName());
    private final Map<String, StorageRecord> store;
    private final Object lock = new Object();

    public Storage() {
        this.store = new ConcurrentHashMap<>();
    }

    public void set(String key, byte[] value, ValueType type, Instant expiry) {
        synchronized(lock) {
            logger.info(Thread.currentThread().getName() + " Setting key: " + key + " with value: " + new String(value));
            logger.info("Current store size before set: " + store.size());
            store.put(key, new StorageRecord(type, value, expiry));
            logger.info("Current store size after set: " + store.size());
            logger.info("Store contents after set: " + store);
        }
    }

    public StorageRecord get(String key) {
        synchronized(lock) {
            logger.info(Thread.currentThread().getName() + " Getting key: " + key);
            logger.info("Current store size: " + store.size());
            StorageRecord record = store.get(key);
            logger.info("Found record: " + (record != null ? new String(record.data()) : "null"));
            if (record != null && record.expiry() != null) {
                Instant now = Instant.now();
                if (now.isAfter(record.expiry())) {
                    store.remove(key);
                    return null;
                }
            }
            return record;
        }
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
        synchronized(lock) {
            logger.info(Thread.currentThread().getName() + " Setting store: " + store);
            logger.info("Current store size before update: " + this.store.size());
            this.store.clear();  // First clear
            this.store.putAll(store);  // Then add all
            logger.info("Current store size after update: " + this.store.size());
            logger.info("Store contents after update: " + this.store);
        }
    }
}
