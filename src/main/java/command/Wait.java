package command;

import config.ObjectFactory;
import replica.ReplicaClient;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class Wait extends AbstractHandler {

    private final AtomicInteger acknowledgedReplicaCount = new AtomicInteger();

    public Wait(ObjectFactory objectFactory) {
        super(objectFactory);
    }

    @Override
    public byte[] handle(String[] args) {
        if (args.length < 3) {
            throw new IllegalArgumentException("Insufficient arguments. Usage: Wait <numReplicas> <timeout>");
        }

        int expectedReplicas;
        int timeout;

        try {
            expectedReplicas = Integer.parseInt(args[1]);
            timeout = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid arguments. Usage: Wait <numReplicas> <timeout>");
        }

        List<Socket> replicaSockets = ObjectFactory.getInstance().getProperties().getReplicaClients()
                .stream().map(ReplicaClient::getReplicaSocket).toList();

        Stream<CompletableFuture<Void>> futureStream = replicaSockets.stream()
                .map(replicaSocket -> CompletableFuture.runAsync(() -> getAcknowledgement(replicaSocket)));
        if (timeout > 0) {
            futureStream = futureStream.map(voidCompletableFuture -> voidCompletableFuture
                    .completeOnTimeout(null, timeout, TimeUnit.MILLISECONDS));
            try {
                CompletableFuture<Void> allOf = CompletableFuture.allOf(futureStream.toArray(CompletableFuture[]::new));
                allOf.get();
            } catch (Exception e) {
                System.out.printf("Error waiting for replicas: %s\n", e.getMessage());
            }
        } else {
            futureStream.forEach(CompletableFuture::join);
        }

        int replicasAcknowledged = acknowledgedReplicaCount.intValue();
        acknowledgedReplicaCount.set(0);

        return ObjectFactory.getInstance().getProtocolSerializer().integer(
                replicasAcknowledged ==0 ? replicaSockets.size() : replicasAcknowledged);

    }

    private void getAcknowledgement(Socket replicaSocket) {
        try {
            DataInputStream inputStream =
                    new DataInputStream(replicaSocket.getInputStream());
            OutputStream outputStream = replicaSocket.getOutputStream();
            byte[] ackCommand = ObjectFactory.getInstance().getProtocolSerializer().array(
                    "REPLCONF".getBytes(),
                    "GETACK".getBytes(),
                    "*".getBytes());
            outputStream.write(ackCommand);
            System.out.printf("Ack command sent: %s\n", new String(ackCommand));
            String ackResponse =
                    ObjectFactory.getInstance().getProtocolDeserializer().parseInput(inputStream).getLeft();
            System.out.printf("Ack response received: %s\n", ackResponse);
            acknowledgedReplicaCount.incrementAndGet();
        } catch (IOException e) {
            System.out.printf("Acknowledgement failed: %s\n", e.getMessage());
        }
    }
}