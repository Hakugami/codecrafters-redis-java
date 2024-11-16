package replica;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ReplicaClient {
    private static final Logger logger = Logger.getLogger(ReplicaClient.class.getName());
    private final AsynchronousSocketChannel channel;
    private final String replicaAddress;
    private final int replicaPort;

    public ReplicaClient(Socket socket) throws IOException {
        this.replicaAddress = socket.getInetAddress().getHostAddress();
        this.replicaPort = socket.getPort();
        this.channel = AsynchronousSocketChannel.open();
        this.channel.connect(socket.getRemoteSocketAddress());
        logger.info("Connected to replica at " + replicaAddress + ":" + replicaPort);
    }

    public CompletableFuture<Void> send(byte[] data) {
        logger.info("Sending data to replica at " + replicaAddress + ":" + replicaPort);
        return writeAsync(data);
    }

    public CompletableFuture<byte[]> sendAndAwaitResponse(byte[] data, long timeout) {
        logger.info("Sending data to replica at " + replicaAddress + ":" + replicaPort);
        
        return writeAsync(data)
            .thenCompose(v -> readAsync())
            .orTimeout(timeout, TimeUnit.MILLISECONDS)
            .exceptionally(throwable -> {
                if (throwable instanceof java.util.concurrent.TimeoutException) {
                    logger.warning("Timeout while waiting for response from replica at " + replicaAddress + ":" + replicaPort);
                    return null;
                }
                throw new RuntimeException("Error communicating with replica", throwable);
            });
    }

    private CompletableFuture<Void> writeAsync(byte[] data) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        
        channel.write(buffer, buffer, new CompletionHandler<Integer, ByteBuffer>() {
            @Override
            public void completed(Integer result, ByteBuffer attachment) {
                if (attachment.hasRemaining()) {
                    // Continue writing if there's more data
                    channel.write(attachment, attachment, this);
                } else {
                    future.complete(null);
                }
            }

            @Override
            public void failed(Throwable exc, ByteBuffer attachment) {
                future.completeExceptionally(exc);
            }
        });

        return future;
    }

    private CompletableFuture<byte[]> readAsync() {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        ByteBuffer buffer = ByteBuffer.allocate(8192); // Adjust size as needed
        
        channel.read(buffer, buffer, new CompletionHandler<Integer, ByteBuffer>() {
            private final ByteBuffer accumulator = ByteBuffer.allocate(16384);

            @Override
            public void completed(Integer result, ByteBuffer attachment) {
                if (result == -1) {
                    // Connection closed
                    future.complete(getAccumulatedData());
                    return;
                }

                attachment.flip();
                accumulator.put(attachment);
                
                if (isResponseComplete()) {
                    future.complete(getAccumulatedData());
                } else {
                    // Continue reading
                    attachment.clear();
                    channel.read(attachment, attachment, this);
                }
            }

            @Override
            public void failed(Throwable exc, ByteBuffer attachment) {
                future.completeExceptionally(exc);
            }

            private boolean isResponseComplete() {
                // Implement Redis protocol response completion check
                // For simple string responses, check for \r\n
                byte[] data = accumulator.array();
                int len = accumulator.position();
                return len >= 2 && data[len - 2] == '\r' && data[len - 1] == '\n';
            }

            private byte[] getAccumulatedData() {
                accumulator.flip();
                byte[] data = new byte[accumulator.remaining()];
                accumulator.get(data);
                return data;
            }
        });

        return future;
    }

    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            logger.warning("Error closing channel: " + e.getMessage());
        }
    }
}
