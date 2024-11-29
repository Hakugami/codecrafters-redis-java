package replica;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Pair;

import command.Handler;
import command.factory.CommandFactory;

import java.util.Map;
import java.io.ByteArrayInputStream;

import config.ObjectFactory;
import config.ReplicaProperties;
import protocol.ProtocolDeserializer;
import protocol.persistence.RDBLoader;
import storage.StorageRecord;

public class ReplicaRunner extends Thread {
    private static final Logger logger = Logger.getLogger(ReplicaRunner.class.getName());

    @Override
    public void run() {
        logger.info("Starting replica " + ObjectFactory.getInstance().getProperties().getPort());
        ReplicaProperties replicaProperties = ObjectFactory.getInstance().getProperties().getReplicaProperties();

        try (Socket masterSocket = new Socket(replicaProperties.host(), replicaProperties.port());
             OutputStream outputStream = masterSocket.getOutputStream();
             DataInputStream dataInputStream = new DataInputStream(masterSocket.getInputStream())) {
            initReplica(outputStream, dataInputStream);
            processCommands(outputStream,dataInputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void initReplica(OutputStream outputStream, DataInputStream dataInputStream) throws IOException {
        logger.info("Initializing replica");

        // Step 1: PING
        sendCommand(outputStream, "PING");
        String response = parseSimpleString(dataInputStream);
        logger.info("Received response from master: " + response);
        if (!response.equals("PONG")) {
            throw new RuntimeException("Failed to initialize replica");
        }

        // Step 2: REPLCONF listening-port
        sendCommand(outputStream, "REPLCONF", "listening-port",
                String.valueOf(ObjectFactory.getInstance().getProperties().getPort()));
        response = parseSimpleString(dataInputStream);
        if (!response.equals("OK")) {
            throw new RuntimeException("Failed to send REPLCONF listening-port");
        }

        // Step 3: REPLCONF capa psync2
        sendCommand(outputStream, "REPLCONF", "capa", "psync2");
        response = parseSimpleString(dataInputStream);
        if (!response.equals("OK")) {
            throw new RuntimeException("Failed to send REPLCONF capa");
        }

        // Step 4: PSYNC
        sendCommand(outputStream, "PSYNC", "?", "-1");
        response = parseSimpleString(dataInputStream);
        logger.info("PSYNC response: " + response);
        if (!response.startsWith("FULLRESYNC")) {
            throw new RuntimeException("Failed to initialize replica: " + response);
        }

        // Step 5: Read the RDB bulk string directly
        // Read '$'
        int c = dataInputStream.read();
        if (c != '$') {
            throw new RuntimeException("Expected '$', got: " + (char) c);
        }

        // Read length
        StringBuilder lengthBuilder = new StringBuilder();
        while ((c = dataInputStream.read()) != '\r') {
            if (c == -1) {
                throw new EOFException("Unexpected end of stream while reading length");
            }
            lengthBuilder.append((char) c);
        }
        // Consume '\n'
        dataInputStream.read();

        int length = Integer.parseInt(lengthBuilder.toString());
        logger.info("RDB data length: " + length);

        // Read RDB data
        byte[] rdbData = new byte[length];
        dataInputStream.readFully(rdbData);

        // Process RDB data
        try {
            DataInputStream rdbInputStream = new DataInputStream(new ByteArrayInputStream(rdbData));
            RDBLoader rdbLoader = ObjectFactory.getInstance().getPersistenceManager().getRdbLoader();
            Map<String, StorageRecord> records = rdbLoader.readAllPairsFromStream(rdbInputStream);
            ObjectFactory.getInstance().getPersistenceManager().getStorage().setStore(records);
            logger.info("Replica initialized successfully with " + records.size() + " records");
        } catch (IOException e) {
            logger.severe("Error processing RDB data: " + e.getMessage());
        }
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

    private String parseSimpleString(DataInputStream dataInputStream) throws IOException {
        // Expect and consume '+'
        int c = dataInputStream.read();
        if (c != '+') {
            throw new RuntimeException("Expected '+', got: " + (char) c);
        }
        StringBuilder stringBuilder = new StringBuilder();
        while ((c = dataInputStream.read()) != -1) {
            if (c == '\r') {
                // Consume '\n'
                dataInputStream.read();
                break;
            }
            stringBuilder.append((char) c);
        }
        return stringBuilder.toString();
    }

    private void processCommands(OutputStream outputStream, DataInputStream dataInputStream) {
        ProtocolDeserializer deserializer = ObjectFactory.getInstance().getProtocolDeserializer();
        CommandFactory commandFactory = ObjectFactory.getInstance().getCommandFactory();
    
        while (true) {
            try {
                Pair<String, Long> parsedInput = deserializer.parseInput(dataInputStream);
                String commandLine = parsedInput.getLeft();
                logger.info("Received command from master: " + commandLine);

                // Skip processing responses (they start with '+')
                if (commandLine.startsWith("+")) {
                    logger.fine("Received response from master: " + commandLine);
                    continue;
                }
                
                // If the message starts with "ERR", it's an error message - just log it and continue
                if (commandLine.startsWith("ERR")) {
                    logger.warning("Received error from master: " + commandLine);
                    continue;
                }
    
                String[] args = commandLine.split(" ");
                Handler handler = commandFactory.getHandler(args[0].toUpperCase());
                if (handler != null) {
                    logger.info("Processing command: " + Arrays.toString(args) + " with handler: " + handler.getClass().getName());
                    byte[] response = handler.handle(args);
                    long added = ObjectFactory.getInstance().getProperties().getReplicationOffset().addAndGet(parsedInput.getRight());
                    logger.info("Processed command: " + Arrays.toString(args) + " with offset: " + added);
                    // Send response back to master
                    outputStream.write(response);
                    outputStream.flush();
                } else {
                    logger.warning("No handler found for command: " + args[0]);
                }
            } catch (IOException e) {
                logger.severe("End of stream reached while processing commands.");
                break;
            } catch (Exception e) {
                logger.severe("Error processing command from master: " + e.getMessage());
                break;
            }
        }
    }
}
