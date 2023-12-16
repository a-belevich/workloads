package org.workloads;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.AbstractActorWithTimers;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;

import static java.time.temporal.ChronoUnit.NANOS;


// service:
// checks for limits (if configured) of active requests
// executes concurrency limiter (if configured)
// invokes downstream services (if configured)
// on error retries (if configured) with backoff (if configured) or returns the error upstream
// waits (simulating computation)
// returns a response
public class Service extends AbstractActorWithTimers {

    private static class Tick {
    }

    enum Concurrency {
        Limited,
        Unlimited,
    }

    private ActorRef downstream;
    private Duration calcDuration;
    private boolean error;

    private Concurrency concurrency;
    private int availableConcurrency;
    private int concurrencyLimit;

    private int downstreamRetries = 3;
    private Duration downstreamMinBackoff = Duration.ofMillis(100);
    private Duration downstreamMaxBackoff = Duration.ofMillis(100);
    private Duration downstreamTimeout = Duration.ofSeconds(1);

    private LocalDateTime created;
    private LocalDateTime lastTick;

    private static class InProgress {
        Request request;
        double msToWait;
    }

    private record SendDownstream(Request request){}
    private static class  InDownstream {
        public Request request;
        public int attempt;
        public LocalDateTime deadline;
        public InDownstream(Request request, int attempt, LocalDateTime deadline) {
            this.request = request;
            this.attempt = attempt;
            this.deadline = deadline;
        }
    }

    private List<InProgress> inProgress = new ArrayList<>();
    private Map<Request.RequestId, InDownstream> inDownstream = new HashMap<>();
    private LinkedList<Request> waiting = new LinkedList<>();

    public static Props props(ActorRef downstream, Concurrency concurrency, int availableConcurrency, int concurrencyLimit, Duration calcDuration, boolean error) {
        return Props.create(Service.class, () -> new Service(downstream, concurrency, availableConcurrency, concurrencyLimit, calcDuration, error));
    }

    public Service(ActorRef downstream, Concurrency concurrency, int availableConcurrency, int concurrencyLimit, Duration calcDuration, boolean error) {
        this.downstream = downstream;
        this.concurrency = concurrency;
        this.availableConcurrency = availableConcurrency;
        this.concurrencyLimit = concurrencyLimit;
        this.calcDuration = calcDuration;
        this.error = error;

        var rnd = new Random().nextInt(1000000);
        created = LocalDateTime.now().minus(Duration.ofMillis(rnd)); //
        lastTick = LocalDateTime.now();
        this.timers().startTimerWithFixedDelay("ms", new Tick(), Duration.ofMillis(1));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Request.class, r -> handleRequest(r))
                .match(SendDownstream.class, d -> sendDownstream(d))
                .match(Response.class, r -> handleResponse(r))
                .match(Tick.class, t -> tick())
                .build();
    }

    private void tick() {
        var now = LocalDateTime.now();
        var sinceLastTick = NANOS.between(lastTick, now);
        lastTick = now;

        double computeProgressedBy = sinceLastTick / 1000000;
        if (this.inProgress.size() > this.availableConcurrency) {
            computeProgressedBy = computeProgressedBy / this.inProgress.size() * this.availableConcurrency;
        }

        var inProgressIter = this.inProgress.iterator();
        while (inProgressIter.hasNext()) {
            var r = inProgressIter.next();
            r.msToWait -= computeProgressedBy;
            if (r.msToWait <= 0) {
                var resp = new Response();
                resp.request = r.request;
                resp.isSuccess = !this.error;
                var last = resp.request.returnPath.removeLast();
                last.tell(resp, this.getSelf());
                inProgressIter.remove();
            }
        }

        var inDownstreamIter = this.inDownstream.entrySet().iterator();
        while(inDownstreamIter.hasNext()) {
            var e = inDownstreamIter.next();
            var req = e.getValue();
            if (!req.deadline.isBefore(now)) {
                continue;
            }

            if (req.attempt >= this.downstreamRetries) {
                inDownstreamIter.remove();
            } else {
                resendDownstream(req, now);
            }
        }

        while (this.concurrency == Concurrency.Unlimited || (inProgress.size() + inDownstream.size()) < this.concurrencyLimit) {
            var next = this.waiting.poll();
            if (next == null)
                break;
            if (this.downstream == null) {
                startCalculation(next);
            } else {
                var d = new InDownstream(next, 0, now.plus(this.downstreamTimeout));
                inDownstream.put(next.id, d);
                self().tell(d, ActorRef.noSender());
            }
        }
    }

    private void resendDownstream(InDownstream req, LocalDateTime now) {
        var backoff = this.downstreamMinBackoff;
        for (int i = 0; i < req.attempt; i++) {
            backoff = backoff.multipliedBy(2);
        }
        if (backoff.compareTo(this.downstreamMaxBackoff) > 0) {
            backoff = this.downstreamMaxBackoff;
        }

        req.attempt++;
        req.deadline = now.plus(this.downstreamTimeout);

        var d = new SendDownstream(req.request.goDownstream(req.attempt));
        if (backoff.isZero()) {
            self().tell(d, ActorRef.noSender());
        } else {
            this.timers().startSingleTimer(req.request.id.id(), d, backoff);
        }
    }

    private void startCalculation(Request r) {
        var ip = new InProgress();
        ip.request = r;
        ip.msToWait = this.calcDuration.toMillis();
        this.inProgress.add(ip);
    }

    private void sendDownstream(SendDownstream d) {
        d.request.returnPath.add(self());
        this.downstream.tell(d.request, getSelf());
    }

    private void handleResponse(Response r) {
        var last = r.request.returnPath.removeLast();
        if (!last.equals(this.getSelf())) {
            throw new RuntimeException("Routing error");
        }
        var inD = inDownstream.get(r.request.id);
        if (inD.attempt != r.request.attempt) {
            return;
        }
        if (r.isSuccess) {
            this.startCalculation(r.request);
            return;
        }
        resendDownstream(inD, LocalDateTime.now());
    }

    private void handleRequest(Request r) {
        this.waiting.push(r);
    }
}
