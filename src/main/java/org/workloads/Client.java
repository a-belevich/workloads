package org.workloads;

import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;

import java.time.Duration;

public class Client extends Service {
    public static Props props(ActorRef downstream) {
        return Props.create(Client.class, () -> new Client(downstream));
    }

    public Client(ActorRef downstream) {
        super(downstream,  1, null, null, Duration.ZERO, false);
    }
}
