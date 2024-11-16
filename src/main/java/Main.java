import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Logger;

import command.ConnectionHandler;
import config.ApplicationProperties;
import config.ObjectFactory;
import replica.ReplicaRunner;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws IOException {
        ObjectFactory objectFactory = ObjectFactory.getInstance(args);
        logger.info("Starting server on port " + objectFactory.getProperties().getPort());

        try (ServerSocket serverSocket = new ServerSocket(objectFactory.getProperties().getPort())) {
            serverSocket.setReuseAddress(true);
            if (objectFactory.getProperties().isReplica()) {
                new ReplicaRunner().start();
            }else{
                logger.info("Master server started");
            }
            while (true) {
                Socket socket = serverSocket.accept();
                logger.info("Client connected " + socket.getInetAddress().getHostAddress());
                new ConnectionHandler(socket, objectFactory).start();
            }
        } catch (IOException e) {
            logger.severe("Failed to start server " + e);

        }
    }
}
