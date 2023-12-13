package org.workloads;

import org.apache.pekko.actor.ActorRef;

import java.time.LocalDateTime;
import java.util.List;

public class Response {
    public List<ActorRef> returnPath;
    public LocalDateTime created;

    public boolean isSuccess;
    public int attemptCount;
}
