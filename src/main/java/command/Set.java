package command;

import config.ObjectFactory;
import protocol.ValueType;
import storage.Storage;

public class Set extends AbstractHandler {
    private final Storage storage;

    public Set(ObjectFactory objectFactory) {
        super(objectFactory);
        this.storage = objectFactory.getStorage();
    }

    @Override
    public byte[] handle(String[] args) {
        if (args.length < 3) {
            return protocolSerializer.simpleError("ERR wrong number of arguments for 'set' command");
        }

        String key = args[1];
        String value = args[2];
        logger.info("Set key: " + key + ", value: " + value);
        storage.set(key, value.getBytes(), ValueType.STRING);

        return protocolSerializer.simpleString("OK");
    }
}
