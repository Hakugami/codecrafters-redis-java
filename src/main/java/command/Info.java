package command;

import config.ObjectFactory;

public class Info extends AbstractHandler {
    public Info(ObjectFactory objectFactory) {
        super(objectFactory);
    }

    @Override
    public byte[] handle(String[] args) {
        return protocolSerializer.bulkStrings("role:master");
    }
}
