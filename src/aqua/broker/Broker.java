package aqua.broker;

import aqua.common.Direction;
import aqua.common.Properties;
import aqua.common.msgtypes.DeregisterRequest;
import aqua.common.msgtypes.RegisterRequest;
import aqua.common.msgtypes.PoisonPill;
import aqua.common.msgtypes.HandoffRequest;
import aqua.common.msgtypes.RegisterResponse;

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

        private void register(Message message) {
            lock.writeLock().lock();
            String id = CLIENT_BASE_ID + (clientCollection.size() + 1);
            clientCollection.add(id, message.getSender());
            lock.writeLock().unlock();

            endpoint.send(message.getSender(), new RegisterResponse(id));
        }

        private void deregister(Message message) {
            lock.writeLock().lock();
            clientCollection.remove(clientCollection.indexOf(message.getSender()));
            lock.writeLock().unlock();
        }

        private void handoffFish(Message message) {
            HandoffRequest payload = (HandoffRequest) message.getPayload();
            Direction direction = payload.getFish().getDirection();

            lock.readLock().lock();
            InetSocketAddress neighbor = switch (direction) {
                case LEFT -> clientCollection.getLeftNeighorOf(clientCollection.indexOf(message.getSender()));
                case RIGHT -> clientCollection.getRightNeighorOf(clientCollection.indexOf(message.getSender()));
            };
            lock.readLock().unlock();

            endpoint.send(neighbor, payload);
        }

        @Override
        public void run() {
            Serializable payload = message.getPayload();

            if (payload instanceof RegisterRequest) {
                register(message);
            }

            if (payload instanceof DeregisterRequest) {
                deregister(message);
            }

            if (payload instanceof HandoffRequest) {
                handoffFish(message);
            }
        }
    }
}
