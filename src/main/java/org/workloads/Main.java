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
            bottom.add(as.actorOf(Service.props(null, 100, new Limiter.LimiterByErrors(Limiter.Reaction.Discard, 200), Duration.ofMillis(100), null), "bottom_"+i));
        }
        bottom.add(as.actorOf(Service.props(null, 100, new Limiter.LimiterByErrors(Limiter.Reaction.Discard, 200), Duration.ofMillis(100), new Errors.OnceInAwhile(Duration.ofSeconds(2))), "bottom_bad"));
        allActors.addAll(bottom);

        var bottomEnvoy = as.actorOf(Group.props(bottom, Group.Balancing.LeastBusyEnvoy), "bottom_envoy");
        allActors.add(bottomEnvoy);

        var top = new ArrayList<ActorRef>();
        for (int i = 0; i < 100; i++) {
            top.add(as.actorOf(Service.props(bottomEnvoy, 100, new Limiter.LimiterByErrors(Limiter.Reaction.Discard, 200), Duration.ofMillis(100), null), "top_"+i));
        }
        allActors.addAll(top);
        var topEnvoy = as.actorOf(Group.props(top, Group.Balancing.LeastBusyEnvoy), "top_envoy");
        allActors.add(topEnvoy);

        var clients = new ArrayList<ActorRef>();
        for (int i = 0; i < 10000; i++) {
            clients.add(as.actorOf(Client.props(topEnvoy), "client_" + i));
        }
        allActors.addAll(clients);

        var driver = as.actorOf(Driver.props(clients, 10, allActors), "driver");
        driver.tell(new Driver.Start(), ActorRef.noSender());
        sleep(300000);

        as.terminate();
    }
}