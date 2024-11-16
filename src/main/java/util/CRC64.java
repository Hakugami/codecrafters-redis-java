package util;

public class CRC64 {
    private static final long[] CRC64_TABLE = new long[256];
    private static final long POLY = 0xC96C5795D7870F42L; // ISO polynomial

    static {
        for (int b = 0; b < 256; b++) {
            long crc = b;
            for (int i = 0; i < 8; i++) {
                if ((crc & 1L) != 0) {
                    crc = (crc >>> 1) ^ POLY;
                } else {
                    crc = crc >>> 1;
                }
            }
            CRC64_TABLE[b] = crc;
        }
    }

    public static long checksum(byte[] data) {
        long crc = 0xFFFFFFFFFFFFFFFFL;
        for (byte b : data) {
            int idx = ((int) crc ^ b) & 0xFF;
            crc = CRC64_TABLE[idx] ^ (crc >>> 8);
        }
        return crc ^ 0xFFFFFFFFFFFFFFFFL;
    }
}
