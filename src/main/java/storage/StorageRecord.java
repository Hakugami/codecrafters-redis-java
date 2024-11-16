package storage;

import protocol.ValueType;

import java.time.Instant;

public record StorageRecord(ValueType type, byte[] data, Instant expiry) {
    public StorageRecord(ValueType type, byte[] data) {
        this(type, data, null);
    }

}
