package org.workloads;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedList;

import static java.lang.Math.floor;

public abstract class Limiter {

    public enum Reaction {
        Wait,
        Discard,
    }

    private Reaction reaction;
    private LinkedList<Request> waiting = new LinkedList<>();
    protected int inFlight;

    protected abstract boolean canStart();

    public abstract void tick(LocalDateTime now);

    public Limiter(Reaction reaction) {
        this.reaction = reaction;
    }

    public Request poll() {
        if (!canStart())
            return null;
        return waiting.poll();
    }

    public boolean push(Request request) {
        if (reaction == Reaction.Wait || canStart()) {
            this.waiting.push(request);
            this.inFlight++;
            return true;
        }

        return false;
    }

    public void hasResult(Response response) {
        this.inFlight--;
    }

    public static class StaticLimiter extends Limiter {
        private int limit;

        public StaticLimiter(Reaction reaction, int limit) {
            super(reaction);
            this.limit = limit;
        }

        @Override
        public void tick(LocalDateTime now) {}

        @Override
        protected boolean canStart() {
            return inFlight < this.limit;
        }
    }

    public static class Unlimited extends Limiter {
        public Unlimited() {
            super(Reaction.Discard);
        }

        @Override
        public void tick(LocalDateTime now) {}

        @Override
        protected boolean canStart() {
            return true;
        }
    }

    public abstract static class AIMDLimiter extends Limiter {
        protected int currentLimit;
        protected int topLimit;
        protected boolean reachedTop;
        protected LocalDateTime nextCheck;
        protected Duration checkFrequency = Duration.ofSeconds(1);

        public AIMDLimiter(Reaction reaction, int topLimit) {
            super(reaction);
            this.topLimit = topLimit;
            this.currentLimit = topLimit / 2;
            if (currentLimit < 1)
                currentLimit = 1;
            this.nextCheck = LocalDateTime.now().plus(checkFrequency);
        }

        protected abstract int moveLimit();

        @Override
        public void tick(LocalDateTime now) {
            if (now.isBefore(nextCheck))
                return;
            nextCheck = now.plus(checkFrequency);

            var move = moveLimit();
            reachedTop = false;

            if (move == 0) {
                return;
            }
            if (move < 0) {
                currentLimit = (int) floor(0.9 * currentLimit);
                if (currentLimit < 1) {
                    this.currentLimit = 1;
                }
                return;
            }
            if (currentLimit < topLimit) {
                currentLimit++;
            }
        }

        @Override
        protected boolean canStart() {
            if (inFlight + 1 >= currentLimit) {
                reachedTop = true;
            }
            return inFlight < this.currentLimit;
        }
    }

    public static class LimiterByErrors extends AIMDLimiter {
        private int thisSecondErrors;

        public LimiterByErrors(Reaction reaction, int topLimit) {
            super(reaction, topLimit);
        }

        @Override
        public int moveLimit() {
            var lastSecondErrors = thisSecondErrors;
            thisSecondErrors = 0;

            if (lastSecondErrors > 0) {
                return -1;
            }
            if (reachedTop) {
                return 1;
            }
            return  0;
        }

        @Override
        public void hasResult(Response response) {
            if (response.status != Response.Status.Ok) {
                thisSecondErrors++;
            }
            super.hasResult(response);
        }
    }

    public static class LimiterByLatency extends AIMDLimiter {
        private int thisSecondResponses;
        private long thisSecondLatencyNanos;

        private Duration decreaseTrigger;
        private Duration increaseTrigger;

        public LimiterByLatency(Reaction reaction, int topLimit, Duration decreaseTrigger, Duration increaseTrigger) {
            super(reaction, topLimit);
            this.decreaseTrigger = decreaseTrigger;
            this.increaseTrigger = increaseTrigger;

            if (increaseTrigger.compareTo(decreaseTrigger) > 0) {
                throw new RuntimeException("Decrease trigger cannot be larger than increase trigger.");
            }
        }

        @Override
        public int moveLimit() {
            long avgLatency = 0;
            if (thisSecondResponses > 0) {
                avgLatency = thisSecondLatencyNanos/thisSecondResponses;
            }
            thisSecondResponses = 0;
            thisSecondLatencyNanos = 0;

            if (avgLatency > decreaseTrigger.toNanos()) {
                return -1;
            }
            if (reachedTop && avgLatency < increaseTrigger.toNanos()) {
                return 1;
            }
            return  0;
        }

        @Override
        public void hasResult(Response response) {
            if (response.status != Response.Status.Discarded) {
                thisSecondResponses++;
                thisSecondLatencyNanos += Duration.between(response.request.created, LocalDateTime.now()).toNanos();
            }
            super.hasResult(response);
        }
    }
}
