package command;

import config.ObjectFactory;

public class Wait extends AbstractHandler {
    public Wait(ObjectFactory objectFactory) {
        super(objectFactory);
    }

    @Override
    public byte[] handle(String[] args) {
        return protocolSerializer.integer(0);
    }
}
