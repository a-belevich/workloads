package org.workloads;

import java.time.Duration;
import java.time.LocalDateTime;

import static java.lang.Math.floor;

public abstract class Limiter {
    public abstract boolean acquire();

    public abstract void hasResult(Response response);

    public abstract void tick(LocalDateTime now);

    public abstract static class AIMDLimiter extends Limiter {
        protected int inFlight;
        protected int currentLimit;
        protected int topLimit;
        protected boolean reachedTop;
        protected LocalDateTime nextCheck;
        protected Duration checkFrequency = Duration.ofSeconds(1);

        public AIMDLimiter(int topLimit) {
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
        public boolean acquire() {
            if (inFlight + 1 >= currentLimit) {
                reachedTop = true;
            }
            var result = inFlight < this.currentLimit;
            if (result) {
                inFlight++;
            }
            return result;
        }

        @Override
        public void hasResult(Response response) {
            this.inFlight--;
        }
    }

    public static class LimiterByErrors extends AIMDLimiter {
        private int thisSecondErrors;

        public LimiterByErrors(int topLimit) {
            super(topLimit);
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
        private int thisSecondErrors;

        public LimiterByLatency(int topLimit) {
            super(topLimit);
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
}
