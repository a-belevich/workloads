package org.workloads;

import org.apache.pekko.actor.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static java.time.temporal.ChronoUnit.NANOS;

public class Driver extends AbstractActorWithTimers {

    static class Tick {}
    public final static class Start {}
    public final static class Stop {}

    private LocalDateTime nextReport;
    private int sent;
    private int successes;
    private int failures;
    private long totalLatencySuccesses;
    private long totalLatencyFailed;

    private int ratePerMs;

    private List<ActorRef> clients;
    private int nextClient = 0;
    List<ActorRef> allActors;

    static Props props(List<ActorRef> clients, int ratePerMs, List<ActorRef> allActors) {
        // You need to specify the actual type of the returned actor
        // since Java 8 lambdas have some runtime type information erased
        return Props.create(Driver.class, () -> new Driver(clients, ratePerMs, allActors));
    }

    public Driver(List<ActorRef> clients, int ratePerMs, List<ActorRef> allActors) {
        this.clients = clients;
        this.ratePerMs = ratePerMs;
        this.allActors = allActors;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Start.class, r -> start())
                .match(Tick.class, t -> tick(t))
                .match(Response.class, r -> response(r))
                .match(Stop.class, r -> stop())
                .build();
    }

    private void response(Response r) {
        var latency = NANOS.between(r.request.created, LocalDateTime.now());
        if (r.status == Response.Status.Ok) {
            successes++;
            totalLatencySuccesses += latency;
        } else {
            failures++;
            totalLatencyFailed += latency;
        }
    }

    private void tick(Tick t) {
        for (var a : allActors) {
            a.tell(t, ActorRef.noSender());
        }

        for (int i = 0; i < ratePerMs; i++) {
            var r = new Request(0);
            r.returnPath.add(getSelf());
            clients.get(nextClient).tell(r, getSelf());
            nextClient = (nextClient + 1) % clients.size();
            this.sent++;
        }

        if (LocalDateTime.now().isAfter(nextReport)) {
            nextReport = nextReport.plus(Duration.ofSeconds(1));
            var avgLatencySuccess = successes == 0 ? 0 : totalLatencySuccesses / successes / 1000000;
            var avgLatencyFailed = failures == 0 ? 0 : totalLatencyFailed / failures / 1000000;
            System.out.println(String.format("Sent %d; succeeded %d (latency %d); failed %d (latency %d).", sent, successes, avgLatencySuccess, failures, avgLatencyFailed));
            sent = 0;
            successes = 0;
            failures = 0;
            totalLatencySuccesses = 0;
            totalLatencyFailed = 0;
        }
    }

    private void start() {
        nextReport = LocalDateTime.now().plus(Duration.ofSeconds(1));
        getTimers().startTimerAtFixedRate("client", new Tick(), Duration.ofMillis(1));
    }

    private void stop() {
        getTimers().cancel("client");
    }
}
