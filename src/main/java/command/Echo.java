package command;

import config.ObjectFactory;

public class Echo extends AbstractHandler {
    public Echo(ObjectFactory objectFactory) {
        super(objectFactory);
    }

    @Override
    public byte[] handle(String[] args) {
        return protocolSerializer.simpleString(args[1]);
    }
}
