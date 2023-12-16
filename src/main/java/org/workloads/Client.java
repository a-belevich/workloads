package org.workloads;

import org.apache.pekko.actor.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;

import static java.time.temporal.ChronoUnit.NANOS;

public class Client extends AbstractActor {

    public final static class Start {
    }

    public final static class Stop {
    }

    public final static class Send {
    }

    private Cancellable cancellable;

    private LocalDateTime nextReport;
    private int sent;
    private int successes;
    private int failures;
    private long totalLatencySuccesses;
    private long totalLatencyFailed;

    private ActorRef downstream;

    static Props props(ActorRef downstream) {
        // You need to specify the actual type of the returned actor
        // since Java 8 lambdas have some runtime type information erased
        return Props.create(Client.class, () -> new Client(downstream));
    }

    public Client(ActorRef downstream) {
        this.downstream = downstream;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Start.class, r -> start())
                .match(Send.class, r -> send())
                .match(Response.class, r -> response(r))
                .match(Stop.class, r -> stop())
                .build();
    }

    private void response(Response r) {
        var latency = NANOS.between(r.request.created, LocalDateTime.now());
        if (r.isSuccess) {
            successes++;
            totalLatencySuccesses += latency;
        } else {
            failures++;
            totalLatencyFailed += latency;
        }
    }

    private void send() {
        var r = new Request(0);
        r.returnPath.add(getSelf());
        downstream.tell(r, getSelf());
        this.sent++;

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
        cancellable = getContext().system().scheduler().scheduleAtFixedRate(
                Duration.ZERO,
                Duration.ofNanos(100000),
                getSelf(),
                new Send(),
                getContext().system().dispatcher(),
                ActorRef.noSender());
    }

    private void stop() {
        cancellable.cancel();
    }
}
