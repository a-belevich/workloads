package org.workloads;

import org.apache.pekko.actor.ActorRef;

import java.time.LocalDateTime;
import java.util.List;

public class Response {

    public enum Status {
        Ok,
        Discarded,
        Error,
        DownstreamError,
    }

    public Request request;
    public Status status;
    public int attemptCount;
}
