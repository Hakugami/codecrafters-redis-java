package protocol.persistence;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;


import config.ObjectFactory;
import org.apache.commons.lang3.tuple.Pair;
import protocol.ValueType;
import storage.StorageRecord;

public class RDBLoader {
    private static final Logger LOGGER = Logger.getLogger(RDBLoader.class.getName());


    // Special Opcodes
    private static final byte RDB_OPCODE_AUX = (byte) 0xFA;
    private static final byte RDB_OPCODE_RESIZEDB = (byte) 0xFB;
    private static final byte RDB_OPCODE_EXPIRETIME_MS = (byte) 0xFC;
    private static final byte RDB_OPCODE_EXPIRETIME = (byte) 0xFD;
    private static final byte RDB_OPCODE_SELECTDB = (byte) 0xFE;
    private static final byte RDB_OPCODE_EOF = (byte) 0xFF;


    public Map<String, StorageRecord> readAllPairs() throws IOException {
        String fullFilename = String.format("%s/%s",
                ObjectFactory.getInstance().getProperties().getDir(),
                ObjectFactory.getInstance().getProperties().getDbFileName());

        try (DataInputStream inputStream = new DataInputStream(new FileInputStream(fullFilename))) {
            // Skip magic and version
            skipMagicAndVersion(inputStream);

            // Skip to RESIZEDB
            byte b = inputStream.readByte();
            while ((b & RDB_OPCODE_RESIZEDB) != RDB_OPCODE_RESIZEDB) {
                b = inputStream.readByte();
            }

            // Read hash table sizes
            readLengthEncodedInt(inputStream); // db size
            readLengthEncodedInt(inputStream); // expiry size

            // Read key-value pairs
            Map<String, StorageRecord> result = new HashMap<>();
            try {
                while (true) {
                    Pair<String, StorageRecord> keyValuePair = readKeyValuePair(inputStream);
                    if (keyValuePair == null) {
                        continue;
                    }
                    result.put(keyValuePair.getKey(), keyValuePair.getValue());
                }
            } catch (EOFException e) {
                LOGGER.info("End of RDB file reached");
            }
            return result;
        } catch (FileNotFoundException e) {
            LOGGER.info("RDB file is not present");
            return Map.of();
        }
    }

    private void skipMagicAndVersion(DataInputStream inputStream) throws IOException {
        byte[] magic = new byte[5];
        inputStream.readFully(magic);
        byte[] version = new byte[4];
        inputStream.readFully(version);
    }

    private Pair<String, StorageRecord> readKeyValuePair(DataInputStream inputStream) throws IOException {
        byte first = inputStream.readByte();
        Instant expiry = Instant.MAX;
        byte valueTypeByte;

        if ((first & RDB_OPCODE_EXPIRETIME) == RDB_OPCODE_EXPIRETIME) {
            int seconds = 0;
            for (int i = 0; i < 4; i++) {
                seconds += ((inputStream.readByte() & 0xFF) << 8 * i);
            }
            expiry = Instant.ofEpochSecond(seconds);
            valueTypeByte = inputStream.readByte();
        } else if ((first & RDB_OPCODE_EXPIRETIME_MS) == RDB_OPCODE_EXPIRETIME_MS) {
            long millis = 0;
            for (int i = 0; i < 8; i++) {
                millis += ((long) (inputStream.readByte() & 0xFF) << 8 * i);
            }
            expiry = Instant.ofEpochMilli(millis);
            valueTypeByte = inputStream.readByte();
        } else if ((first & RDB_OPCODE_EOF) == RDB_OPCODE_EOF) {
            throw new EOFException();
        } else {
            valueTypeByte = first;
        }

        byte[] key = readEncodedString(inputStream);
        byte[] value;
        ValueType valueType;

        if (valueTypeByte == 0) {
            valueType = ValueType.STRING;
            value = readEncodedString(inputStream);
        } else if ((valueTypeByte & 0xFF) == 0xFF) {
            throw new EOFException();
        } else {
            LOGGER.info("Value type is not implemented: " + valueTypeByte);
            return null;
        }

        return Pair.of(new String(key),
                new StorageRecord(valueType, value, expiry));
    }

    private int readLengthEncodedInt(DataInputStream inputStream) throws IOException {
        final byte TWO_LEFTMOST_BITS = (byte) 0b1100_0000;
        byte first = inputStream.readByte();

        if ((first & TWO_LEFTMOST_BITS) == 0b0000_0000) {
            return first;
        } else if ((first & TWO_LEFTMOST_BITS) == 0b0100_0000) {
            byte second = inputStream.readByte();
            return ((first & 0b0011_1111) << 8) + (second & 0xFF);
        } else if ((first & 0b1000_0000) > 0) {
            int result = 0;
            for (int i = 0; i < 4; i++) {
                result = (result << 8) + (inputStream.readByte() & 0xFF);
            }
            return result;
        } else if ((first & TWO_LEFTMOST_BITS) == TWO_LEFTMOST_BITS) {
            if ((first & 0b0011_1111) == 0) {
                return inputStream.readByte();
            } else if ((first & 0b0011_1111) == 1) {
                int result = 0;
                for (int i = 0; i < 2; i++) {
                    result = (result << 8) + (inputStream.readByte() & 0xFF);
                }
                return result;
            } else if ((first & 0b0011_1111) == 2) {
                int result = 0;
                for (int i = 0; i < 4; i++) {
                    result = (result << 8) + (inputStream.readByte() & 0xFF);
                }
                return result;
            }
        }
        throw new RuntimeException("Unexpected bits: " + first);
    }

    private byte[] readEncodedString(DataInputStream inputStream) throws IOException {
        int stringSize = readLengthEncodedInt(inputStream);
        ByteArrayOutputStream buf = new ByteArrayOutputStream(stringSize);
        for (int i = 0; i < stringSize; i++) {
            buf.write(inputStream.readByte());
        }
        return buf.toByteArray();
    }
}