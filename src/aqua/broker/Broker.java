package aqua.broker;

import aqua.common.Direction;
import aqua.common.msgtypes.DeregisterRequest;
import aqua.common.msgtypes.RegisterRequest;
import aqua.common.msgtypes.HandoffRequest;
import aqua.common.msgtypes.RegisterResponse;
import messaging.Endpoint;
import messaging.Message;

import java.io.Serializable;
import java.net.InetSocketAddress;

public class Broker {

    private final Endpoint endpoint = new Endpoint(4711);
    private final ClientCollection<InetSocketAddress> clientCollection = new ClientCollection<>();

    public Broker() {}

    public void broker() {
        while (true) {
            Message message = endpoint.blockingReceive();
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

    private void register(Message message) {
        String id = "tank" + (clientCollection.size() + 1);

        clientCollection.add(id, message.getSender());
        System.out.println("registering " + id + " to " + message.getSender());
        endpoint.send(message.getSender(), new RegisterResponse(id));
    }

    private void deregister(Message message) {
        clientCollection.remove(clientCollection.indexOf(message.getSender()));
    }

    private void handoffFish(Message message) {
        HandoffRequest payload = (HandoffRequest) message.getPayload();
        Direction direction = payload.getFish().getDirection();

        InetSocketAddress neighbor = switch (direction) {
            case LEFT -> clientCollection.getLeftNeighorOf(clientCollection.indexOf(message.getSender()));
            case RIGHT -> clientCollection.getRightNeighorOf(clientCollection.indexOf(message.getSender()));
        };

        endpoint.send(neighbor, payload);
    }

    public static void main(String[] args) {
        Broker broker = new Broker();
        broker.broker();
    }
}
