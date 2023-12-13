package org.workloads;

import org.apache.pekko.actor.*;

import java.util.ArrayList;

import static java.lang.Thread.sleep;

public class Main {
    public static void main(String[] args) throws InterruptedException {

        ActorSystem as = ActorSystem.create();

        var bottom = new ArrayList<ActorRef>();
        for (int i = 0; i < 100; i++) {
            bottom.add(as.actorOf(Service.props(null, 100000000, false)));
        }
        bottom.add(as.actorOf(Service.props(null, 100000000, true)));

        var envoy = as.actorOf(Group.props(bottom));

        var client = as.actorOf(Client.props(envoy));
        client.tell(new Client.Start(), ActorRef.noSender());
        sleep(100000);

        as.terminate();
    }
}