package command;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import config.ObjectFactory;
import protocol.persistence.PersistenceManager;

public class Keys extends AbstractHandler {
    private final PersistenceManager persistenceManager;

    public Keys(ObjectFactory objectFactory) {
        super(objectFactory);
        this.persistenceManager = objectFactory.getPersistenceManager();
    }

    @Override
    public byte[] handle(String[] args) {
        if (args.length != 2) {
            return protocolSerializer.simpleError("ERR wrong number of arguments for 'keys' command");
        }

        String pattern = args[1];
        Set<String> allKeys = persistenceManager.getStorage().getAllKeys();
        List<String> matchedKeys = new ArrayList<>();

        try {
            // Handle special case for "*" pattern
            if (pattern.equals("*")) {
                matchedKeys.addAll(allKeys);
            } else {
                // Convert Redis pattern to regex pattern
                String regex = convertRedisPatternToRegex(pattern);
                Pattern compiledPattern = Pattern.compile(regex);

                for (String key : allKeys) {
                    if (compiledPattern.matcher(key).matches()) {
                        matchedKeys.add(key);
                    }
                }
            }

            // Convert matched keys to byte arrays
            byte[][] keyArrays = matchedKeys.stream()
                    .map(String::getBytes)
                    .toArray(byte[][]::new);

            return protocolSerializer.array(keyArrays);
        } catch (Exception e) {
            logger.severe("Error processing KEYS command: " + e.getMessage());
            return protocolSerializer.simpleError("ERR " + e.getMessage());
        }
    }

    private String convertRedisPatternToRegex(String pattern) {
        StringBuilder regex = new StringBuilder();
        boolean escaping = false;

        for (char c : pattern.toCharArray()) {
            if (escaping) {
                regex.append(Pattern.quote(String.valueOf(c)));
                escaping = false;
                continue;
            }

            switch (c) {
                case '\\' -> escaping = true;
                case '*' -> regex.append(".*");
                case '?' -> regex.append(".");
                case '[' -> regex.append("[");
                case ']' -> regex.append("]");
                case '^' -> regex.append("\\^");
                case '$' -> regex.append("\\$");
                case '.' -> regex.append("\\.");
                case '{' -> regex.append("\\{");
                case '}' -> regex.append("\\}");
                case '(' -> regex.append("\\(");
                case ')' -> regex.append("\\)");
                case '+' -> regex.append("\\+");
                case '|' -> regex.append("\\|");
                default -> regex.append(Pattern.quote(String.valueOf(c)));
            }
        }
        
        return regex.toString();
    }
}
