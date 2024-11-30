package command;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Pair;

import config.ObjectFactory;
import replica.ReplicaClient;

public class ConnectionHandler extends Thread {
    private static final Logger logger = Logger.getLogger(ConnectionHandler.class.getName());
    private final Socket socket;
    private final ObjectFactory objectFactory;
    private boolean isReplicaSocket;

    public ConnectionHandler(Socket socket, ObjectFactory objectFactory) {
        this.socket = socket;
        this.objectFactory = objectFactory;
    }

    @Override
    public void run() {
        try {
            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            while (true) {
                Pair<String, Long> parsedInput = objectFactory.getProtocolDeserializer().parseInput(dataInputStream);
                String command = parsedInput.getLeft();
                logger.info("Received command " + command);
                String[] args = command.split(" ");
                logger.info("Command args " + Arrays.toString(args));
                Handler handler = objectFactory.getCommandFactory().getHandler(args[0].toUpperCase());
                byte[] response = handler.handle(args);

                if (handler instanceof Psync) {
                    isReplicaSocket = true;
                    ObjectFactory.getInstance().getProperties().addReplicaClient(new ReplicaClient(socket));
                    dataOutputStream.write(response);
                    dataOutputStream.flush();
                    // Do not return here; continue the loop to keep the connection alive
                    break;
                }

                if (!isReplicaSocket && objectFactory.getProperties().isMaster() && isWriteCommand(args[0])) {
                    objectFactory.getCommandReplicator().replicateWriteCommand(command);
                }

                dataOutputStream.write(response);
            }
        } catch (IOException e) {
            logger.severe("IOException in ConnectionHandler: " + e.getMessage());
        }
    }

    private boolean isWriteCommand(String command) {
        return Set.of("SET", "DEL", "INCR", "DECR", "RPUSH", "LPUSH", "SADD", "ZADD")
                .contains(command.toUpperCase());
    }
}
