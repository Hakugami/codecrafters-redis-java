package command;

import config.ObjectFactory;
import org.apache.commons.lang3.ArrayUtils;
import storage.StorageRecord;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import util.CRC64;

public class Psync extends AbstractHandler {
    private static final byte RDB_OPCODE_SELECTDB = (byte) 0xFE;
    private static final byte RDB_OPCODE_RESIZEDB = (byte) 0xFB;
    private static final byte RDB_OPCODE_EOF = (byte) 0xFF;
    private static final byte RDB_OPCODE_AUX = (byte) 0xFA;
    private static final String REDIS_VERSION = "0011";

    public Psync(ObjectFactory objectFactory) {
        super(objectFactory);
    }

    @Override
    public byte[] handle(String[] args) {
        String replicationId = ObjectFactory.getInstance().getProperties().getReplicationId();
        long replicationOffset = ObjectFactory.getInstance().getProperties().getReplicationOffset();
        String response = String.format("FULLRESYNC %s %s", replicationId, replicationOffset);
        byte[] fullRsyncResponseBytes = ObjectFactory.getInstance().getProtocolSerializer().simpleString(response);
        
        byte[] rdbFile = generateRdbFile();
        byte[] sizePrefix = ("$" + rdbFile.length + "\r\n").getBytes();
        byte[] responseBytes = ArrayUtils.addAll(fullRsyncResponseBytes, sizePrefix);
        responseBytes = ArrayUtils.addAll(responseBytes, rdbFile);
        return responseBytes;
    }

    private byte[] generateRdbFile() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            
            // Write magic string and version
            dos.write("REDIS".getBytes(StandardCharsets.UTF_8));
            dos.write(REDIS_VERSION.getBytes(StandardCharsets.UTF_8));

            // Write AUX fields
            writeAux(dos, "redis-ver", "7.2.0");
            writeAux(dos, "redis-bits", "64");

            // Write SELECTDB
            dos.write(RDB_OPCODE_SELECTDB);
            writeLength(dos, 0);

            // Get current storage data
            Map<String, StorageRecord> data = ObjectFactory.getInstance()
                .getPersistenceManager().getStorage().getStore();

            // Write RESIZEDB
            dos.write(RDB_OPCODE_RESIZEDB);
            writeLength(dos, data.size());
            writeLength(dos, 0); // No expiring keys for now

            // Write key-value pairs
            for (Map.Entry<String, StorageRecord> entry : data.entrySet()) {
                writeString(dos, entry.getKey());
                writeString(dos, new String(entry.getValue().data()));
            }

            // Write EOF
            dos.write(RDB_OPCODE_EOF);

            byte[] rdbData = baos.toByteArray();
            
            // Calculate and append CRC64
            ByteArrayOutputStream finalBaos = new ByteArrayOutputStream();
            finalBaos.write(rdbData);
            writeLengthLittleEndian(finalBaos, CRC64.checksum(rdbData));
            
            return finalBaos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate RDB file", e);
        }
    }

    private void writeAux(DataOutputStream dos, String key, String value) throws Exception {
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
            dos.write(length & 0x3F);
        } else if (length < (1 << 14)) {
            dos.write(((length >> 8) & 0x3F) | 0x40);
            dos.write(length & 0xFF);
        } else {
            dos.write(0x80);
            dos.writeInt(length);
        }
    }

    private void writeLengthLittleEndian(ByteArrayOutputStream baos, long value) throws Exception {
        for (int i = 0; i < 8; i++) {
            baos.write((int) (value & 0xFF));
            value >>= 8;
        }
    }
}
