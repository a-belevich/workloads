package org.workloads;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Random;

public abstract class Errors {
    public abstract boolean error(Request r);

    public static class None extends Errors {
        @Override
        public boolean error(Request r) {
            return false;
        }
    }

    public static class Always extends Errors {
        @Override
        public boolean error(Request r) {
            return true;
        }
    }

    public static class RandomPercentage extends Errors {
        private double share;
        private Random rnd = new Random();

        public RandomPercentage(double share) {
            this.share = share;
        }

        @Override
        public boolean error(Request r) {
            return (rnd.nextDouble() < share);
        }
    }

// TODO
//    public static class DeterministicPercentage extends Errors {
//        private double share;
//
//        public DeterministicPercentage(double share) {
//            this.share = share;
//        }
//
//        @Override
//        public boolean error(Request r) {
//            return (rnd.nextDouble() < share);
//        }
//    }

    public static class OnceInAwhile extends Errors {
        private Duration period;
        private LocalDateTime nextFire;

        public OnceInAwhile(Duration period) {
            this.period = period;
            nextFire = LocalDateTime.now().plus(period);
        }

        @Override
        public boolean error(Request r) {
            if (nextFire.isAfter(LocalDateTime.now())) {
                nextFire = LocalDateTime.now().plus(period);
                return true;
            }
            return false;
        }
    }
}
