package protocol;

import org.apache.commons.lang3.tuple.Pair;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.stream.IntStream;

public class ProtocolDeserializer {
    public Pair<String, Long> parseInput(DataInputStream dataInputStream) throws EOFException {
        try {
            // Don't check available() as it's unreliable for socket streams
            int firstByte;
            try {
                firstByte = dataInputStream.read();
            } catch (EOFException e) {
                throw e; // Propagate EOFException directly
            }

            if (firstByte == -1) {
                throw new EOFException("End of stream reached");
            }

            char c = (char) firstByte;
            var parsedResult = switch (c) {
                case '*' -> parseArray(dataInputStream);
                case '$' -> parseBulkString(dataInputStream);
                case '+' -> parseSimpleString(dataInputStream);
                default -> throw new RuntimeException("Invalid input character: " + c);
            };
            return Pair.of(parsedResult.getLeft(), parsedResult.getRight() + 1);
        } catch (EOFException e) {
            throw e; // Propagate EOFException instead of wrapping
        } catch (IOException e) {
            throw new RuntimeException("IO error occurred", e);
        }
    }

    private Pair<String, Long> parseBulkString(DataInputStream dataInputStream) throws EOFException {
        Pair<Integer, Long> pair = parseDigits(dataInputStream);
        if (pair.getLeft() == -1) {
            return Pair.of("null", pair.getRight());
        }

        try {
            StringBuilder stringBuilder = new StringBuilder();
            long bytesRead = 0;

            for (int i = 0; i < pair.getLeft(); i++) {
                int b = dataInputStream.read();
                if (b == -1) {
                    throw new EOFException("Unexpected end of stream while reading bulk string");
                }
                stringBuilder.append((char) b);
                bytesRead++;
            }

            // Read \r\n
            int cr = dataInputStream.read();
            int lf = dataInputStream.read();
            if (cr == -1 || lf == -1) {
                throw new EOFException("Unexpected end of stream while reading terminator");
            }
            if (cr != '\r' || lf != '\n') {
                throw new RuntimeException("Invalid bulk string terminator");
            }

            bytesRead += 2;
            return Pair.of(stringBuilder.toString(), bytesRead);
        } catch (IOException e) {
            throw new RuntimeException("Error parsing bulk string", e);
        }
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

    public Pair<Integer, Long> parseDigits(DataInputStream dataInputStream) throws EOFException {
        try {
            Pair<String, Long> pair = parseSimpleString(dataInputStream);
            return Pair.of(Integer.parseInt(pair.getLeft()), pair.getRight());
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid number format", e);
        }
    }

    private Pair<String, Long> parseArray(DataInputStream dataInputStream) throws EOFException {
        Pair<Integer, Long> pair = parseDigits(dataInputStream);
        if (pair.getLeft() < 0) {
            throw new RuntimeException("Invalid array length: " + pair.getLeft());
        }

        return IntStream.range(0, pair.getLeft())
                .mapToObj(i -> {
                    try {
                        return parseInput(dataInputStream);
                    } catch (EOFException e) {
                        throw new RuntimeException("Unexpected end of stream while reading array element", e);
                    }
                })
                .reduce(Pair.of("", pair.getRight()),
                        (acc, p) -> Pair.of(acc.getLeft() + p.getLeft(),
                                acc.getRight() + p.getRight()));
    }
}