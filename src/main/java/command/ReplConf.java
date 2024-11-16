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
                return protocolSerializer.array(
                    Stream.of("REPLCONF", "ACK", String.valueOf(offset))
                        .map(String::getBytes)
                        .toArray(byte[][]::new)
                );
            }
            default -> {
                return protocolSerializer.simpleError("ERR invalid parameter");
            }
        }
        return protocolSerializer.simpleString("OK");
    }
}
