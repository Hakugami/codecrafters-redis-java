import command.ConnectionHandler;
import config.ApplicationProperties;
import config.ObjectFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Logger;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        ApplicationProperties properties = new ApplicationProperties(args);
        logger.info("Starting server on port " + properties.getPort());
        ObjectFactory objectFactory = new ObjectFactory(properties);

        try (ServerSocket serverSocket = new ServerSocket(properties.getPort())) {
            serverSocket.setReuseAddress(true);
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
