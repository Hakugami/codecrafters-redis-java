package replica;

import config.ObjectFactory;
import config.ReplicaProperties;
import lombok.SneakyThrows;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

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
        byte[] request = ObjectFactory.getInstance().getProtocolSerializer().array(stringArrayToByteArray(List.of("PING").toArray(new String[0])));
        outputStream.write(request);
        String response = ObjectFactory.getInstance().getProtocolDeserializer().parseInput(dataInputStream).getLeft();
        logger.info("Received response from master " + response);
        if (!response.equals("PONG")) {
            throw new RuntimeException("Failed to initialize replica");
        }
        logger.info("Sending REPLCONF ACK to master");
        byte[] ackRequest = ObjectFactory.getInstance().getProtocolSerializer().array(
                stringArrayToByteArray(
                        List.of("REPLCONF",
                                        "Listening-Port",
                                        String.valueOf(ObjectFactory.getInstance().getProperties().getPort()))
                                .toArray(new String[0])));
        outputStream.write(ackRequest);
        if (!ObjectFactory.getInstance().getProtocolDeserializer().parseInput(dataInputStream).getLeft().equals("OK")) {
            throw new RuntimeException("Failed to send REPLCONF ACK to master");
        }
        // REPLCONF capa PSYNC
        logger.info("Sending REPLCONF PSYNC to master");
        byte[] repelconfCapaPsyncRequest = ObjectFactory.getInstance().getProtocolSerializer().array(
                stringArrayToByteArray(
                        List.of("REPLCONF",
                                        "capa",
                                        "psync2")
                                .toArray(new String[0])));
        outputStream.write(repelconfCapaPsyncRequest);
        if (!ObjectFactory.getInstance().getProtocolDeserializer().parseInput(dataInputStream).getLeft().equals("OK")) {
            throw new RuntimeException("Failed to send REPLCONF capa PSYNC to master");
        }

        // PSYNC ? -1
        logger.info("Sending PSYNC ? -1 to master");
        byte[] psyncRequest = ObjectFactory.getInstance().getProtocolSerializer().array(
                stringArrayToByteArray(
                        List.of("PSYNC",
                                        "?",
                                        "-1")
                                .toArray(new String[0])));
        outputStream.write(psyncRequest);
        String psyncResponse = ObjectFactory.getInstance().getProtocolDeserializer().parseInput(dataInputStream).getLeft();
        logger.info("Received response from master " + psyncResponse);
        if (!psyncResponse.equals("FULLRESYNC")) {
            throw new RuntimeException("Failed to initialize replica");
        }
        ObjectFactory.getInstance().getProtocolDeserializer().parseRdbFile(dataInputStream);

        logger.info("Replica initialized");


    }

    private byte[] stringArrayToByteArray(String[] array) {
        return Arrays.stream(array).map(String::getBytes).reduce((bytes, bytes2) -> {
            byte[] result = new byte[bytes.length + bytes2.length];
            System.arraycopy(bytes, 0, result, 0, bytes.length);
            System.arraycopy(bytes2, 0, result, bytes.length, bytes2.length);
            return result;
        }).orElse(new byte[0]);
    }
}
