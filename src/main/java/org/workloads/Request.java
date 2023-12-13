package org.workloads;

import org.apache.pekko.actor.ActorRef;

import java.time.LocalDateTime;
import java.util.List;

public class Request {
    public List<ActorRef> returnPath;
    public LocalDateTime created;
}
