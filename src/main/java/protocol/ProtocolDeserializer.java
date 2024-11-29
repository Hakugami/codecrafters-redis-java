package protocol;

import org.apache.commons.lang3.tuple.Pair;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.logging.Logger;


public class ProtocolDeserializer {
    private static final Logger logger = Logger.getLogger(ProtocolDeserializer.class.getName());

    public Pair<String, Long> parseInput(DataInputStream dataInputStream) throws EOFException {
        try {
            // Read the first byte
            int firstByte = dataInputStream.read();
            if (firstByte == -1) {
                throw new EOFException("End of stream reached");
            }

            char c = (char) firstByte;
            return switch (c) {
                case '*' -> parseArray(dataInputStream);
                case '$' -> parseBulkString(dataInputStream);
                case '+' -> parseSimpleString(dataInputStream);
                case ':' -> parseInteger(dataInputStream);  // Add support for integers
                default -> throw new RuntimeException("Invalid input character: " + c);
            };
        } catch (IOException e) {
            throw new RuntimeException("IO error occurred", e);
        }
    }

    private Pair<String, Long> parseBulkString(DataInputStream dataInputStream) throws IOException {
        Pair<Integer, Long> lengthPair = parseDigits(dataInputStream);
        int length = lengthPair.getLeft();

        // Handle null bulk string
        if (length == -1) {
            return Pair.of("$-1\r\n", lengthPair.getRight());
        }

        // Read length bytes plus \r\n terminator
        int totalLength = length + 2; // Include \r\n
        byte[] bytes = new byte[totalLength];
        int bytesRead = dataInputStream.readNBytes(bytes, 0, totalLength);
        
        if (bytesRead != totalLength) {
            throw new EOFException("Incomplete bulk string: expected " + totalLength + " bytes, got " + bytesRead);
        }

        // Validate terminator
        if (bytes[length] != '\r' || bytes[length + 1] != '\n') {
            logger.severe("Invalid bulk string terminator: " + bytes[length] + " " + bytes[length + 1]);
            logger.severe("Bytes: " + new String(bytes));
            throw new RuntimeException("Invalid bulk string terminator");
        }

        // Convert to string excluding \r\n
        return Pair.of(new String(bytes, 0, length), lengthPair.getRight() + totalLength);
    }

    private Pair<String, Long> parseSimpleString(DataInputStream dataInputStream) throws EOFException {
        try {
            StringBuilder stringBuilder = new StringBuilder();
            long bytesRead = 0;

            while (true) {
                int b = dataInputStream.read();
                if (b == -1) {
                    throw new EOFException("Unexpected end of stream while reading simple string");
                }
                char c = (char) b;
                if (c == '\r') {
                    break;
                }
                stringBuilder.append(c);
                bytesRead++;
            }

            // Read \n
            int lf = dataInputStream.read();
            if (lf == -1) {
                throw new EOFException("Unexpected end of stream while reading terminator");
            }
            if (lf != '\n') {
                throw new RuntimeException("Invalid simple string terminator");
            }

            bytesRead += 2; // Count both \r and \n
            return Pair.of(stringBuilder.toString(), bytesRead);
        } catch (IOException e) {
            throw new RuntimeException("Error parsing simple string", e);
        }
    }

    public Pair<Integer, Long> parseDigits(DataInputStream dataInputStream) throws IOException {
        StringBuilder number = new StringBuilder();
        long bytesRead = 0;

        while (true) {
            int b = dataInputStream.read();
            if (b == -1) {
                throw new EOFException("Unexpected end of stream while reading digits");
            }

            char c = (char) b;
            bytesRead++;

            if (c == '\r') {
                int lf = dataInputStream.read();
                if (lf != '\n') {
                    throw new RuntimeException("Invalid number terminator");
                }
                bytesRead++;
                break;
            }

            // Allow negative numbers
            if (number.isEmpty() && c == '-' || Character.isDigit(c)) {
                number.append(c);
            } else {
                throw new RuntimeException("Invalid character in number: " + c);
            }
        }

        try {
            return Pair.of(Integer.parseInt(number.toString()), bytesRead);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid number format: " + number);
        }
    }

    private Pair<String, Long> parseArray(DataInputStream dataInputStream) throws IOException {
        Pair<Integer, Long> lengthPair = parseDigits(dataInputStream);
        int length = lengthPair.getLeft();

        if (length == -1) {
            return Pair.of("*-1\r\n", lengthPair.getRight());
        }

        if (length < 0) {
            throw new RuntimeException("Invalid array length: " + length);
        }

        StringBuilder result = new StringBuilder();
        long totalBytesRead = lengthPair.getRight();

        for (int i = 0; i < length; i++) {
            Pair<String, Long> element = parseInput(dataInputStream);
            if (i > 0) {
                result.append(" ");
            }
            result.append(element.getLeft());
            totalBytesRead += element.getRight();
        }

        return Pair.of(result.toString(), totalBytesRead);
    }

    // Add support for integer parsing
    private Pair<String, Long> parseInteger(DataInputStream dataInputStream) throws IOException {
        Pair<Integer, Long> pair = parseDigits(dataInputStream);
        return Pair.of(String.valueOf(pair.getLeft()), pair.getRight());
    }

    public String parseRdbFile(DataInputStream inputStream) throws IOException {
        char c = (char) inputStream.readByte();
        if (c != '$') {
            throw new RuntimeException("Unexpected start of RDB file string: " + c);
        }
        int stringLength = parseDigits(inputStream).getLeft();
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < stringLength; i++) {
            stringBuilder.append((char) inputStream.readByte());
        }
        return stringBuilder.toString();
    }



}