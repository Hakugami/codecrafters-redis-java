package command;

import config.ObjectFactory;
import lombok.Getter;
import protocol.ProtocolSerializer;

import java.util.logging.Logger;

@Getter
public abstract class AbstractHandler implements Handler {
    protected static final Logger logger = Logger.getLogger(AbstractHandler.class.getName());
    protected final ProtocolSerializer protocolSerializer;

    public AbstractHandler(ObjectFactory objectFactory) {
        this.protocolSerializer = objectFactory.getProtocolSerializer();
        logger.info("Protocol serializer: " + protocolSerializer);

    }

}
