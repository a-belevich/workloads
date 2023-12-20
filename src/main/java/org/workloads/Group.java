package org.workloads;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Group extends AbstractActor {

    public enum Balancing {
        LeastBusyEnvoy,
        RoundRobinEnvoy,
        ClusterIP,
    }

    public static class Connect{}

    private List<ActorRef> downstream;
    private int counter;
    private Map<ActorRef, Integer> active;
    private Map<ActorRef, ActorRef> connections = new HashMap<>();
    private Balancing balancing;

    static Props props(List<ActorRef> downstream, Balancing balancing) {
        return Props.create(Group.class, () -> new Group(downstream, balancing));
    }

    public Group(List<ActorRef> downstream, Balancing balancing) {
        this.balancing = balancing;
        this.downstream = downstream;
        this.active = new HashMap<>();
        for (var a : downstream) {
            active.put(a, 0);
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Request.class, r -> handleRequest(r))
                .match(Response.class, r -> handleResponse(r))
                .match(Connect.class, c -> handleConnect())
                .match(Driver.Tick.class, t -> tick())
                .build();
    }

    private void handleConnect() {
        if (this.balancing != Balancing.ClusterIP)
            return;
        this.connections.put(getSender(), getRoundRobin());
    }

    private void tick() {
    }

    private void handleResponse(Response r) {
        var last = r.request.returnPath.removeLast();
        if (!last.equals(this.getSelf())) {
            throw new RuntimeException("Routing error");
        }

        var sender = getSender();
        var wasBusy = active.get(sender);
        if (wasBusy <= 0) {
            throw new RuntimeException("Less busy than zero");
        }
        active.put(getSender(), wasBusy - 1);
        r.request.returnPath.getLast().tell(r, getSelf());
    }

    private ActorRef getLeastBusy() {
        var leastBusy = downstream.get(0);
        var leastBusyCount = active.get(leastBusy);
        for (var e : active.entrySet()) {
            if (e.getValue() < leastBusyCount) {
                leastBusyCount = e.getValue();
                leastBusy = e.getKey();
            }
        }
        return leastBusy;
    }

    private ActorRef getRoundRobin() {
        counter = (counter + 1) % this.downstream.size();
        return downstream.get(counter);
    }

    private void handleRequest(Request r) {

        var next = switch (this.balancing) {
            case Balancing.LeastBusyEnvoy -> getLeastBusy();
            case Balancing.RoundRobinEnvoy -> getRoundRobin();
            case Balancing.ClusterIP -> getClusterIP();
            default -> throw new RuntimeException("Unknown balancing");
        };
        var active = this.active.get(next);
        if (active == null) {
            throw new RuntimeException("Didn't find downstream activity stats");
        }
        this.active.put(next, active + 1);

        r.returnPath.add(this.getSelf());
        next.tell(r, getSelf());
    }

    private ActorRef getClusterIP() {
        return connections.get(getSender());
    }
}
