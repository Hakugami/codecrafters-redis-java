package command;

import config.ObjectFactory;
import org.apache.commons.lang3.tuple.Pair;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;
import java.util.logging.Logger;

public class ConnectionHandler extends Thread {
    private static final Logger logger = Logger.getLogger(ConnectionHandler.class.getName());
    private final Socket socket;
    private final ObjectFactory objectFactory;

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
                byte[] response = objectFactory.getCommandFactory().getHandler(args[0].toUpperCase()).handle(args);

                dataOutputStream.write(response);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                logger.warning("Failed to close socket " + e);
            }
        }
    }
}
