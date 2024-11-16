package command;

import config.ObjectFactory;
import storage.StorageRecord;

public class Get extends AbstractHandler {
    private final Storage storage;

    public Get(ObjectFactory objectFactory) {
        super(objectFactory);
        this.storage = objectFactory.getStorage();
    }

    @Override
    public byte[] handle(String[] args) {
        if (args.length != 2) {
            return protocolSerializer.simpleError("ERR wrong number of arguments for 'get' command");
        }
        
        String key = args[1];
        StorageRecord record = storage.get(key);
        
        if (record == null) {
            return protocolSerializer.nullBulkString();
        }
        
        return protocolSerializer.bulkStrings(new String(record.data()));
    }
}
