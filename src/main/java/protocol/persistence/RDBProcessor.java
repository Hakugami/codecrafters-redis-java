package protocol.persistence;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import config.ObjectFactory;
import storage.StorageRecord;
import util.CRC64;

public class RDBProcessor {
    private static final Logger logger = Logger.getLogger(RDBProcessor.class.getName());
    private static final String REDIS_VERSION = "0011";

    // Value Types
    private static final byte RDB_TYPE_STRING = 0;
    private static final byte RDB_TYPE_LIST = 1;
    private static final byte RDB_TYPE_SET = 2;
    private static final byte RDB_TYPE_ZSET = 3;
    private static final byte RDB_TYPE_HASH = 4;

    // Special Opcodes
    private static final byte RDB_OPCODE_EOF = (byte) 0xFF;
    private static final byte RDB_OPCODE_SELECTDB = (byte) 0xFE;
    private static final byte RDB_OPCODE_EXPIRETIME = (byte) 0xFD;
    private static final byte RDB_OPCODE_EXPIRETIME_MS = (byte) 0xFC;
    private static final byte RDB_OPCODE_RESIZEDB = (byte) 0xFB;
    private static final byte RDB_OPCODE_AUX = (byte) 0xFA;

    // String Encoding Types
    private static final byte RDB_ENC_INT8 = (byte) 0xF0;
    private static final byte RDB_ENC_INT16 = (byte) 0xF1;
    private static final byte RDB_ENC_INT32 = (byte) 0xF2;
    private static final byte RDB_ENC_STRING = (byte) 0xF3;

    public void saveAllKeys() {
        String filePath = String.format("%s/%s",
                ObjectFactory.getInstance().getProperties().getDir(),
                ObjectFactory.getInstance().getProperties().getDbFileName());

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream buffer = new DataOutputStream(baos)) {

            // Write magic string and version
            buffer.write("REDIS".getBytes(StandardCharsets.UTF_8));
            buffer.write(REDIS_VERSION.getBytes(StandardCharsets.UTF_8));

            // Write AUX fields
            writeAuxField(buffer, "redis-ver", "7.2.0");
            writeAuxField(buffer, "redis-bits", "64");

            // Write SELECTDB
            buffer.write(RDB_OPCODE_SELECTDB);
            writeLength(buffer, 0);

            // Write RESIZEDB
            Map<String, StorageRecord> data = ObjectFactory.getInstance()
                    .getPersistenceManager().getStorage().getStore();
            buffer.write(RDB_OPCODE_RESIZEDB);
            writeLength(buffer, data.size());
            writeLength(buffer, Math.toIntExact(countExpiringKeys(data)));

            // Write key-value pairs
            for (Map.Entry<String, StorageRecord> entry : data.entrySet()) {
                buffer.write(RDB_TYPE_STRING);
                writeString(buffer, entry.getKey());
                writeString(buffer, new String(entry.getValue().data(), StandardCharsets.UTF_8));
            }

            // Write EOF
            buffer.write(RDB_OPCODE_EOF);

            // Calculate and write CRC64
            byte[] rdbData = baos.toByteArray();
            long crc64 = CRC64.checksum(rdbData);
            
            try (FileOutputStream fos = new FileOutputStream(filePath)) {
                fos.write(rdbData);
                writeLittleEndianLong(fos, crc64);
            }

        } catch (Exception e) {
            logger.severe("Failed to save RDB file: " + e.getMessage());
            throw new RuntimeException("Failed to save RDB file", e);
        }
    }

    private void writeAuxField(DataOutputStream dos, String key, String value) throws Exception {
        dos.write(RDB_OPCODE_AUX);
        writeString(dos, key);
        writeString(dos, value);
    }

    private void writeString(DataOutputStream dos, String str) throws Exception {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        writeLength(dos, bytes.length);
        dos.write(bytes);
    }

    private void writeLength(DataOutputStream dos, int length) throws Exception {
        if (length < (1 << 6)) {
            // 6-bit encoding (00XXXXXX)
            dos.write(length & 0x3F);
        } else if (length < (1 << 14)) {
            // 14-bit encoding (01XXXXXX YYYYYYYY)
            dos.write(((length >> 8) & 0x3F) | 0x40);
            dos.write(length & 0xFF);
        } else {
            // 32-bit encoding (10000000 XXXXXXXX YYYYYYYY ZZZZZZZZ WWWWWWWW)
            dos.write(0x80);
            dos.writeInt(length);
        }
    }

    private void writeLittleEndianLong(FileOutputStream fos, long value) throws Exception {
        for (int i = 0; i < 8; i++) {
            fos.write((int) (value & 0xFF));
            value >>= 8;
        }
    }

    private long countExpiringKeys(Map<String, StorageRecord> data) {
        return data.values().stream()
                .filter(record -> record.expiry() != null)
                .count();
    }

    // These methods should be implemented based on your serialization format
    private List<byte[]> deserializeList(byte[] data) {
        // Implement list deserialization
        throw new UnsupportedOperationException("List deserialization not implemented");
    }

    private Set<byte[]> deserializeSet(byte[] data) {
        // Implement set deserialization
        throw new UnsupportedOperationException("Set deserialization not implemented");
    }

    private Map<byte[], Double> deserializeZSet(byte[] data) {
        // Implement sorted set deserialization
        throw new UnsupportedOperationException("ZSet deserialization not implemented");
    }

    private Map<byte[], byte[]> deserializeHash(byte[] data) {
        // Implement hash deserialization
        throw new UnsupportedOperationException("Hash deserialization not implemented");
    }
}