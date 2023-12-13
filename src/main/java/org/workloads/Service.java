package org.workloads;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;

import java.time.Duration;

public class Service extends AbstractActor {

    static class NoDelayRequest {
        Request request;
        public NoDelayRequest(Request request) {
            this.request = request;
        }
    }

    private ActorRef downstream;
    private int delay;
    private boolean error;

    public static Props props(ActorRef downstream, int delay, boolean error) {
        // You need to specify the actual type of the returned actor
        // since Java 8 lambdas have some runtime type information erased
        return Props.create(Service.class, () -> new Service(downstream, delay, error));
    }

    public Service(ActorRef downstream, int delay, boolean error) {
        this.downstream = downstream;
        this.delay = delay;
        this.error = error;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(NoDelayRequest.class, r -> handleNoDelayRequest(r.request))
                .match(Request.class, r -> handleRequest(r))
                .match(Response.class, r -> handleResponse(r))
                .build();
    }

    private void handleNoDelayRequest(Request r) {
        if (this.downstream == null) {
            var resp = new Response();
            resp.returnPath = r.returnPath;
            resp.created = r.created;
            resp.isSuccess = !error;
            var last = resp.returnPath.removeLast();
            last.tell(resp, this.getSelf());
            return;
        }
        r.returnPath.add(this.getSelf());
        this.downstream.tell(   r, getSelf());
    }

    private void handleResponse(Response r) {
        var last = r.returnPath.removeLast();
        if (!last.equals(this.getSelf())) {
            throw new RuntimeException("Routing error");
        }
    }

    private void handleRequest(Request r) {
        if (error) {
            handleNoDelayRequest(r);
            return;
        }
        getContext().system().scheduler().scheduleOnce(
                Duration.ofNanos(delay),
                getSelf(),
                new NoDelayRequest(r),
                getContext().system().dispatcher(),
                ActorRef.noSender());
    }
}
