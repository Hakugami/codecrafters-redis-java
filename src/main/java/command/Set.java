package command;

import config.ObjectFactory;
import protocol.ValueType;
import storage.Storage;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class Set extends AbstractHandler {
    private final Storage storage;

    public Set(ObjectFactory objectFactory) {
        super(objectFactory);
        this.storage = objectFactory.getPersistenceManager().getStorage();
    }

    @Override
    public byte[] handle(String[] args) {
        logger.info("Handling set command ");
        if (args.length < 3) {
            return protocolSerializer.simpleError("ERR wrong number of arguments for 'set' command");
        }

        String key = args[1];
        String value = args[2];
        Instant expiry = null;

        // Handle expiration if provided
        if (args.length > 3) {
            try {
                expiry = parseExpiry(args[3], args[4]);
            } catch (IllegalArgumentException e) {
                return protocolSerializer.simpleError("ERR " + e.getMessage());
            }
        }

        logger.info("Set key: " + key + ", value: " + value + ", expiry: " + expiry);
        storage.set(key, value.getBytes(), ValueType.STRING, expiry);
        return protocolSerializer.simpleString("OK");
    }

    private Instant parseExpiry(String unit, String value) {
        int expiryValue;
        try {
            expiryValue = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid expire time in set");
        }

        if (expiryValue <= 0) {
            throw new IllegalArgumentException("expire time must be positive");
        }

        return switch (unit.toUpperCase()) {
            case "EX" -> Instant.now().plus(expiryValue, ChronoUnit.SECONDS);
            case "PX" -> Instant.now().plus(expiryValue, ChronoUnit.MILLIS);
            default -> throw new IllegalArgumentException("invalid expire time unit");
        };
    }
}
