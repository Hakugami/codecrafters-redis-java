package command.factory;

import command.*;
import config.ObjectFactory;

import java.lang.reflect.Constructor;
import java.util.EnumMap;
import java.util.Map;
import java.util.logging.Logger;

public class CommandFactory {
    private static final Logger logger = Logger.getLogger(CommandFactory.class.getName());
    private final Map<Command, Handler> handlers = new EnumMap<>(Command.class);

    public Handler getHandler(String command) {
        Handler handler = handlers.get(Command.valueOf(command));
        if (handler == null) {
            throw new IllegalArgumentException("Unknown command: " + command);
        }
        return handler;
    }

    public CommandFactory(ObjectFactory objectFactory) {
        for (Command command : Command.values()) {
            try {
                Constructor<? extends Handler> constructor = command.handler.getConstructor(ObjectFactory.class);
                Handler handler = constructor.newInstance(objectFactory);
                logger.info("Instantiated handler for " + command);
                logger.info("Handler: " + handler);
                handlers.put(command, handler);
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Failed to instantiate handler for " + command, e);
            }
        }
    }


    private enum Command {
        PING(Ping.class),
        ECHO(Echo.class),
        SET(Set.class),
        GET(Get.class),
        CONFIG(Config.class),
        KEYS(Keys.class),
        SAVE(Save.class),
        INFO(Info.class),;

        private final Class<? extends Handler> handler;

        Command(Class<? extends Handler> handler) {
            this.handler = handler;
        }
    }
}


