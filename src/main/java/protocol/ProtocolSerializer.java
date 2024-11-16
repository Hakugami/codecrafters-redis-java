package protocol;

public class ProtocolSerializer {
    private static final String CRLF = "\r\n";

    public byte[] simpleString(String value) {
        return ("+" + value + CRLF).getBytes();
    }

    public byte[] simpleError(String message) {
        return ("-" + message + CRLF).getBytes();
    }

    public byte[] bulkStrings(String value) {
        return ("$" + value.length() + CRLF + value + CRLF).getBytes();
    }

    public byte[] integer(long value) {
        return (":" + value + CRLF).getBytes();
    }

    public byte[] array(byte[]... values) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(values.length).append(CRLF);
        for (byte[] value : values) {
            sb.append("$").append(value.length).append(CRLF);
            sb.append(new String(value)).append(CRLF);
        }
        return sb.toString().getBytes();
    }

    public byte[] nullArray() {
        return "*-1\r\n".getBytes();
    }

    public byte[] nullBulkString() {
        return "$-1\r\n".getBytes();
    }
}
