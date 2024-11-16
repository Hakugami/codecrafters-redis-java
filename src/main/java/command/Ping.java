package command;

import config.ObjectFactory;

public class Ping extends AbstractHandler {
    public Ping(ObjectFactory objectFactory) {
        super(objectFactory);
    }

    @Override
    public byte[] handle(String[] args) {
        return protocolSerializer.simpleString("PONG");
    }
}
