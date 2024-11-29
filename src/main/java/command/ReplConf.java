package command;

import config.ObjectFactory;
import org.apache.commons.lang3.math.NumberUtils;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class ReplConf extends AbstractHandler {
    public ReplConf(ObjectFactory objectFactory) {
        super(objectFactory);
    }

    @Override
    public byte[] handle(String[] args) {
        String parameter = args[1].toLowerCase();
        logger.info("Handling REPLCONF command with parameter: " + parameter);
        switch (parameter) {
            case "listening-port" -> {
                if (!NumberUtils.isDigits(args[2])) {
                    return protocolSerializer.simpleError("ERR invalid port");
                }
            }
            case "capa" -> {
                if (!Set.of("psync2", "eof").contains(args[2].toLowerCase())) {
                    return protocolSerializer.simpleError("ERR invalid capability");
                }
            }
            case "getack" -> {
                long offset = ObjectFactory.getInstance().getProperties().getReplicationOffset();
                // Format as RESP array with 3 elements: REPLCONF, ACK, <offset>
                return protocolSerializer.array(new byte[][] {
                        "REPLCONF".getBytes(),
                        "ACK".getBytes(),
                        String.valueOf(offset).getBytes()
                });
            }
            default -> {
                return protocolSerializer.simpleError("ERR invalid parameter");
            }
        }
        return protocolSerializer.simpleString("OK");
    }
}
