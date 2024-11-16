package command;

import config.ObjectFactory;

public class Info extends AbstractHandler {
    public Info(ObjectFactory objectFactory) {
        super(objectFactory);
    }

    @Override
    public byte[] handle(String[] args) {
        if(ObjectFactory.getInstance().getProperties().getReplicaProperties()==null) {
            return protocolSerializer.bulkStrings("role:master");
        }
        return protocolSerializer.bulkStrings("role:slave");
    }
}
