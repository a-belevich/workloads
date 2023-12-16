package org.workloads;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.AbstractActorWithTimers;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;

import static java.lang.Math.floor;
import static java.time.temporal.ChronoUnit.NANOS;

/*
 * Service class sends a downstream request (unless it's the leaf); and then runs some computation (handles the results).
 */
public class Service extends AbstractActorWithTimers {

    private static class Tick {
    }

    public record Executors(int numOfExecutors){}

    public abstract static  class Limiter{
        public abstract boolean acquire();
        public abstract void hasResult(Response response);
    }

    // if there was an error last second, decrease concurrency by 10%; if there were no errors and worked at max, increase by 1
    public static class LimiterByErrors extends Limiter{

        private int inFlight;
        private int thisSecondLimit;

        private int thisSecondErrors;
        private int lastSecondErrors;
        private boolean reachedTop;

        private LocalDateTime nextCheck;
        private Duration checkFrequency = Duration.ofSeconds(1);

        public LimiterByErrors(int initialLimit) {
            this.thisSecondLimit = initialLimit;
            this.nextCheck = LocalDateTime.now().plus(checkFrequency);
        }

        private void checkForStep() {
            var now = LocalDateTime.now();
            if (now.isBefore(nextCheck))
                return;
            lastSecondErrors = thisSecondErrors;
            thisSecondErrors = 0;
            reachedTop = false;
            nextCheck = now.plus(checkFrequency);

            if (lastSecondErrors > 0) {
                thisSecondLimit = (int) floor(0.9 * thisSecondLimit);
                if (thisSecondLimit < 1) {
                    this.thisSecondLimit = 1;
                }
                return;
            }
            if (this.reachedTop) {
                thisSecondLimit++;
            }
        }

        @Override
        public boolean acquire() {
            checkForStep();

            if (inFlight + 1 >= this.thisSecondLimit) {
                reachedTop = true;
            }
            var result = inFlight < this.thisSecondLimit;
            if (result) {
                inFlight++;
            }
            return result;
        }

        @Override
        public void hasResult(Response response) {
            if (response.status != Response.Status.Ok) {
                thisSecondErrors++;
            }
            this.inFlight--;
        }
    }

    private ActorRef downstream;
    private Duration calcDuration;
    private boolean error;
    private Limiter limiter;

    private int availableConcurrency;
    private Executors executors;

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

    /*
    * downstream - downstream service; may be null.
    * availableConcurrency - number of messages that can be processed locally at full speed. Extra messages handled in parallel delay the execution (emulates CPU bottleneck).
    *   however, the requests waiting for downstream services are not counted against the availableConcurrency.
    * executors - number of executors. if null, the number is unlimited.
    *   if not null, emulates the limit of how many messages are being processed vs how many messages are waiting to start the processing.
    *   can also be used to emulate some limited resource, like connection pool size.
    * duration - duration of local calculation (not counting the time downstream or the time waiting for the executor to pick it up).
    *
    * */
    public static Props props(ActorRef downstream, int availableConcurrency, Executors executors, Limiter limiter, Duration calcDuration, boolean error) {
        return Props.create(Service.class, () -> new Service(downstream, availableConcurrency, executors, limiter, calcDuration, error));
    }

    public Service(ActorRef downstream, int availableConcurrency, Executors executors, Limiter limiter, Duration calcDuration, boolean error) {
        this.downstream = downstream;
        this.availableConcurrency = availableConcurrency;
        this.executors = executors;
        this.limiter = limiter;
        this.calcDuration = calcDuration;
        this.error = error;

        var rnd = new Random().nextInt(1000000);
        created = LocalDateTime.now().minus(Duration.ofMillis(rnd)); //
        lastTick = LocalDateTime.now();
        this.timers().startTimerWithFixedDelay("ms", new Tick(), Duration.ofMillis(1));
    }

    int inFlight() {
        return inProgress.size() + inDownstream.size();
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
                sendResponse(r.request, (this.error ? Response.Status.Error : Response.Status.Ok));
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
                sendResponse(req.request, Response.Status.DownstreamError);
            } else {
                resendDownstream(req, now);
            }
        }

        while (this.executors == null || inFlight() < this.executors.numOfExecutors) {
            var next = this.waiting.poll();
            if (next == null)
                break;
            if (this.downstream == null) {
                startCalculation(next);
            } else {
                var s = new SendDownstream(next.goDownstream(0));
                var d = new InDownstream(next, 0, now.plus(this.downstreamTimeout));
                inDownstream.put(next.id, d);
                self().tell(s, ActorRef.noSender());
            }
        }
    }

    private void sendResponse(Request req, Response.Status status) {
        var resp = new Response();
        resp.request = req;
        resp.status = status;
        var last = resp.request.returnPath.removeLast();
        if (!last.equals(this.getSelf())) {
            throw new RuntimeException("Routing error");
        }

        resp.request.returnPath.getLast().tell(resp, this.getSelf());
        if (status != Response.Status.Discarded && this.limiter != null) {
            this.limiter.hasResult(resp);
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
            self().tell(d, self());
        } else {
            this.timers().startSingleTimer(req.request.id.id() + req.attempt, d, backoff);
        }
    }

    private void startCalculation(Request r) {
        var ip = new InProgress();
        ip.request = r;
        ip.msToWait = this.calcDuration.toMillis();
        this.inProgress.add(ip);
    }

    private void sendDownstream(SendDownstream d) {
        this.downstream.tell(d.request, getSelf());
    }

    private void handleResponse(Response r) {
        var last = r.request.returnPath.getLast();
        if (!last.equals(this.getSelf())) {
            throw new RuntimeException("Routing error");
        }
        var inD = inDownstream.get(r.request.id);
        if (inD == null || inD.attempt != r.request.attempt) {
            return;
        }
        if (r.status == Response.Status.Ok) {
            this.inDownstream.remove(r.request.id);
            this.startCalculation(r.request);
            return;
        }
        if (inD.attempt >= this.downstreamRetries) {
            this.inDownstream.remove(r.request.id);
            sendResponse(inD.request, Response.Status.DownstreamError);
        } else {
            resendDownstream(inD, LocalDateTime.now());
        }
    }

    private void handleRequest(Request r) {
        r.returnPath.add(self());

        if (this.limiter != null && !this.limiter.acquire()) {
            sendResponse(r, Response.Status.Discarded);
            return;
        }
        this.waiting.push(r);
    }
}
