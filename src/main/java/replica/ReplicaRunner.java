package replica;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.logging.Logger;

import config.ObjectFactory;
import config.ReplicaProperties;

public class ReplicaRunner extends Thread {
    private static final Logger logger = Logger.getLogger(ReplicaRunner.class.getName());

    @Override
    public void run() {
        ReplicaProperties replicaProperties = ObjectFactory.getInstance().getProperties().getReplicaProperties();

        try (Socket masterSocket = new Socket(replicaProperties.host(), replicaProperties.port());
             OutputStream outputStream = masterSocket.getOutputStream();
             DataInputStream dataInputStream = new DataInputStream(masterSocket.getInputStream())) {
            initReplica(outputStream, dataInputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void initReplica(OutputStream outputStream, DataInputStream dataInputStream) throws IOException {
        logger.info("Initializing replica");
        
        // Step 1: PING
        sendCommand(outputStream, "PING");
        String response = ObjectFactory.getInstance().getProtocolDeserializer().parseInput(dataInputStream).getLeft();
        logger.info("Received response from master: " + response);
        if (!response.equals("PONG")) {
            throw new RuntimeException("Failed to initialize replica");
        }

        // Step 2: REPLCONF listening-port
        sendCommand(outputStream, "REPLCONF", "listening-port", 
                   String.valueOf(ObjectFactory.getInstance().getProperties().getPort()));
        response = ObjectFactory.getInstance().getProtocolDeserializer().parseInput(dataInputStream).getLeft();
        if (!response.equals("OK")) {
            throw new RuntimeException("Failed to send REPLCONF listening-port");
        }

        // Step 3: REPLCONF capa psync2
        sendCommand(outputStream, "REPLCONF", "capa", "psync2");
        response = ObjectFactory.getInstance().getProtocolDeserializer().parseInput(dataInputStream).getLeft();
        if (!response.equals("OK")) {
            throw new RuntimeException("Failed to send REPLCONF capa");
        }

        // Step 4: PSYNC
        sendCommand(outputStream, "PSYNC", "?", "-1");
        response = ObjectFactory.getInstance().getProtocolDeserializer().parseInput(dataInputStream).getLeft();
        logger.info("PSYNC response: " + response);
        if (!response.startsWith("FULLRESYNC")) {
            throw new RuntimeException("Failed to initialize replica: " + response);
        }

        // Step 5: Read RDB file
        ObjectFactory.getInstance().getProtocolDeserializer().parseRdbFile(dataInputStream);
        logger.info("Replica initialized successfully");
    }

    private void sendCommand(OutputStream outputStream, String... args) throws IOException {
        StringBuilder command = new StringBuilder();
        command.append("*").append(args.length).append("\r\n");
        
        for (String arg : args) {
            command.append("$").append(arg.length()).append("\r\n");
            command.append(arg).append("\r\n");
        }
        
        outputStream.write(command.toString().getBytes());
        outputStream.flush();
        logger.fine("Sent command: " + String.join(" ", args));
    }
}
