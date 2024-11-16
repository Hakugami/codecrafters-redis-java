package command;

import config.ObjectFactory;

public class Psync extends AbstractHandler {
    public Psync(ObjectFactory objectFactory) {
        super(objectFactory);
    }

    @Override
    public byte[] handle(String[] args) {
        return "ERR unknown subcommand".getBytes();
    }
}
