package aqua.broker;

import aqua.common.Direction;
import aqua.common.Properties;
import aqua.common.msgtypes.*;

import messaging.Endpoint;
import messaging.Message;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Broker {

    private static final String CLIENT_BASE_ID = "tank";
    private static final int NUM_THREADS = 5;

    private final Endpoint endpoint = new Endpoint(Properties.PORT);
    private final ClientCollection<InetSocketAddress> clientCollection = new ClientCollection<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

    public Broker() {}

    public void broker() {
        while (true) {
            Message message = endpoint.blockingReceive();

            if (message.getPayload() instanceof PoisonPill) {
                break;
            }
            BrokerTask task = new BrokerTask(message);
            executor.execute(task);
        }

        executor.shutdown();
    }

    public static void main(String[] args) {
        Broker broker = new Broker();
        broker.broker();
    }

    private class BrokerTask implements Runnable {
        private final Message message;

        BrokerTask(Message message) {
            this.message = message;
        }

        private void register(InetSocketAddress newClient) {
            lock.writeLock().lock();
            String id = CLIENT_BASE_ID + (clientCollection.size() + 1);
            clientCollection.add(id, newClient);
            lock.writeLock().unlock();

            lock.readLock().lock();
            int indexOfNewClient = clientCollection.indexOf(newClient);
            InetSocketAddress leftNeighbor = clientCollection.getLeftNeighorOf(indexOfNewClient);
            InetSocketAddress rightNeighbor = clientCollection.getRightNeighorOf(indexOfNewClient);
            lock.readLock().unlock();

            endpoint.send(newClient, new NeighborUpdate(rightNeighbor, Direction.RIGHT));
            endpoint.send(newClient, new NeighborUpdate(leftNeighbor, Direction.LEFT));

            endpoint.send(leftNeighbor, new NeighborUpdate(newClient, Direction.RIGHT));
            endpoint.send(rightNeighbor, new NeighborUpdate(newClient, Direction.LEFT));

            endpoint.send(newClient, new RegisterResponse(id));
        }

        private void deregister(InetSocketAddress client) {
            lock.writeLock().lock();
            clientCollection.remove(clientCollection.indexOf(client));
            lock.writeLock().unlock();

            lock.readLock().lock();
            int indexOfClient = clientCollection.indexOf(client);
            InetSocketAddress leftNeighbor = clientCollection.getLeftNeighorOf(indexOfClient);
            InetSocketAddress rightNeighbor = clientCollection.getRightNeighorOf(indexOfClient);
            lock.readLock().unlock();

            endpoint.send(client, new NeighborUpdate(client, Direction.LEFT));
            endpoint.send(client, new NeighborUpdate(client, Direction.LEFT));

            endpoint.send(leftNeighbor, new NeighborUpdate(rightNeighbor, Direction.RIGHT));
            endpoint.send(rightNeighbor, new NeighborUpdate(leftNeighbor, Direction.LEFT));
        }

        @Override
        public void run() {
            if (message.getPayload() instanceof RegisterRequest)
                register(message.getSender());

            if (message.getPayload() instanceof DeregisterRequest)
                deregister(message.getSender());
        }
    }
}
