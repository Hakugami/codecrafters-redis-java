package replica;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

public class ReplicaClient {
    private static final Logger logger = Logger.getLogger(ReplicaClient.class.getName());

    private final Socket replicaSocket;

    private final ExecutorService executorService;

    private final Object socketLock = new Object();

    public ReplicaClient(Socket replicaSocket) {
        this.replicaSocket = replicaSocket;
        this.executorService = Executors.newSingleThreadExecutor();
    }

    public void send(byte[] data) {
        logger.info("Sending data to replica, size: " + data.length);
        logger.info("Data content: " + new String(data));
        synchronized(socketLock) {
            executorService.execute(() -> doSend(data));
        }
    }

    public byte[] sendAndAwaitResponse(byte[] data, long timeout) {
        logger.info("Sending data to replica at port" + replicaSocket.getPort() + "the command is " + new String(data));
        Future<byte[]> future = executorService.submit(()-> doSendAndAwait(data));
        try {
            return future.get(timeout, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            logger.warning("Timeout while waiting for response from replica at port " + replicaSocket.getPort());
            future.cancel(true);
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] doSendAndAwait(byte[] data) {
        try {
            OutputStream outputStream = replicaSocket.getOutputStream();
            outputStream.write(data);
            logger.info("Data sent to replica at port " + replicaSocket.getPort());
            outputStream.flush();
            BufferedReader reader = new BufferedReader(new InputStreamReader(replicaSocket.getInputStream()));
            logger.info("Waiting for response from replica at port " + replicaSocket.getPort());
            String response ;
            while ((response = reader.readLine()) == null) {
                Thread.sleep(100);
            }
            logger.info("Received response from replica at port " + replicaSocket.getPort());
            return response.getBytes();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void doSend(byte[] data) {
        synchronized(socketLock) {
            try {
                OutputStream outputStream = replicaSocket.getOutputStream();
                outputStream.write(data);
                logger.info("Data sent to replica at port " + replicaSocket.getPort());
                outputStream.flush();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public Socket getReplicaSocket() {
        return replicaSocket;
    }
}
