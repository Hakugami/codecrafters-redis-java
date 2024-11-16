package replica;

import config.ObjectFactory;

import java.util.Arrays;
import java.util.List;

public class CommandReplicator {
    public void replicateWriteCommand(String command) {
        ObjectFactory.getInstance().getProperties().getReplicaClients().forEach(replicaClient ->
                replicate(replicaClient, command));
    }

    private void replicate(ReplicaClient replicaClient, String command) {
        byte[] commands = Arrays.stream(command.split(" "))
                .map(String::getBytes)
                .reduce((bytes, bytes2) -> {
                    byte[] result = new byte[bytes.length + bytes2.length];
                    System.arraycopy(bytes, 0, result, 0, bytes.length);
                    System.arraycopy(bytes2, 0, result, bytes.length, bytes2.length);
                    return result;
                }).orElse(new byte[0]);
        replicaClient.send(commands);
    }
}
