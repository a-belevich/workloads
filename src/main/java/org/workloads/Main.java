package org.workloads;

import org.apache.pekko.actor.*;

import java.time.Duration;
import java.util.ArrayList;

import static java.lang.Thread.sleep;

public class Main {
    public static void main(String[] args) throws InterruptedException {

        ActorSystem as = ActorSystem.create();

        var allActors = new ArrayList<ActorRef>();
        var bottom = new ArrayList<ActorRef>();
        for (int i = 0; i < 100; i++) {
            bottom.add(as.actorOf(Service.props(null, 100, null, new Limiter.LimiterByErrors(Limiter.Reaction.Discard, 200), Duration.ofMillis(100), false)));
        }
//        bottom.add(as.actorOf(Service.props(null, 100, null, new Limiter.LimiterByErrors(200), Duration.ofMillis(100), true)));
        allActors.addAll(bottom);

        var bottomEnvoy = as.actorOf(Group.props(bottom, Group.Balancing.LeastBusyEnvoy));
        allActors.add(bottomEnvoy);

        var top = new ArrayList<ActorRef>();
        for (int i = 0; i < 100; i++) {
            // top.add(as.actorOf(Service.props(bottomEnvoy, 100, new Service.Executors(200), null, Duration.ofMillis(100), false)));
            top.add(as.actorOf(Service.props(bottomEnvoy, 100, null, new Limiter.LimiterByErrors(Limiter.Reaction.Discard, 200), Duration.ofMillis(100), false)));
        }
        allActors.addAll(top);
        var topEnvoy = as.actorOf(Group.props(top, Group.Balancing.LeastBusyEnvoy));
        allActors.add(topEnvoy);

        var clients = new ArrayList<ActorRef>();
        for (int i = 0; i < 10000; i++) {
            clients.add(as.actorOf(Client.props(topEnvoy)));
        }
        allActors.addAll(clients);

        var driver = as.actorOf(Driver.props(clients, 10, allActors));
        driver.tell(new Driver.Start(), ActorRef.noSender());
        sleep(100000);

        as.terminate();
    }
}