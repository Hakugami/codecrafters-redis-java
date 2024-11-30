package command;

import config.ObjectFactory;

public class Wait extends AbstractHandler {
    public Wait(ObjectFactory objectFactory) {
        super(objectFactory);
    }

    @Override
    public byte[] handle(String[] args) {
        if (args.length < 3) {
            throw new IllegalArgumentException("Insufficient arguments. Usage: Wait <numReplicas> <timeout>");
        }

        int requestedReplicas = Integer.parseInt(args[1]);
        String timeout = args[2];

        // If 0 replicas requested, return immediately
        if (requestedReplicas == 0) {
            return protocolSerializer.integer(0);
        }

        try {
            long timeoutMillis = Long.parseLong(timeout);
            Thread.sleep(timeoutMillis);

            // Get actual number of replicas
            int actualReplicas = ObjectFactory.getInstance().getProperties().getReplicaClients().size();

            // Return minimum between requested and actual replicas
            return protocolSerializer.integer(Math.min(requestedReplicas, actualReplicas));

        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid timeout value: " + timeout, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread was interrupted while waiting", e);
        }
    }
}