package org.workloads;

import org.apache.pekko.actor.ActorRef;

import java.time.LocalDateTime;
import java.util.List;

public class Response {
    public Request request;
    public boolean isSuccess;
    public int attemptCount;
}
