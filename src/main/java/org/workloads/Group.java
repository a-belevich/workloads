package org.workloads;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Group extends AbstractActor {

    private List<ActorRef> downstream;
    private int counter;
    private Map<ActorRef, Integer> active;

    static Props props(List<ActorRef> downstream) {
        // You need to specify the actual type of the returned actor
        // since Java 8 lambdas have some runtime type information erased
        return Props.create(Group.class, () -> new Group(downstream));
    }

    public Group(List<ActorRef> downstream) {
        this.downstream = downstream;
        this.active = new HashMap<>();
        for(var a : downstream) {
            active.put(a, 0);
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Request.class, r -> handleRequest(r))
                .match(Response.class, r -> handleResponse(r))
                .build();
    }

    private void handleResponse(Response r) {
        var last = r.request.returnPath.removeLast();
        var wasBusy = active.get(getSender());
        if (wasBusy <= 0) {
            throw new RuntimeException("Less busy than zero");
        }
        active.put(getSender(), wasBusy - 1);
        last.tell(r, getSelf());
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
        active.put(leastBusy, leastBusyCount + 1);
        return  leastBusy;
    }

    private ActorRef getRoundRobin() {
        counter = (counter+1) % this.downstream.size();
        return downstream.get(counter);
    }


    private void handleRequest(Request r) {
        r.returnPath.add(this.getSelf());
        getLeastBusy().tell(r, getSelf());
    }
}
