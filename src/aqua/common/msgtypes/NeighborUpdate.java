package aqua.common.msgtypes;

import aqua.common.Direction;

import java.io.Serializable;
import java.net.InetSocketAddress;

public class NeighborUpdate implements Serializable {
    private final InetSocketAddress neighborAddress;
    private final Direction neighborSide;

    public NeighborUpdate(InetSocketAddress neighborAddress, Direction neighborSide) {
        this.neighborAddress = neighborAddress;
        this.neighborSide = neighborSide;
    }

    public InetSocketAddress getNeighborAddress() {
        return neighborAddress;
    }

    public Direction getNeighborSide() {
        return neighborSide;
    }
}
