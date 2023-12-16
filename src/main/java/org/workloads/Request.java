package org.workloads;

import org.apache.pekko.actor.ActorRef;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Request {
    public record RequestId(String id){}

    public Request(int attempt) {
        this(new RequestId(UUID.randomUUID().toString()), attempt);
    }

    public Request(RequestId id, int attempt) {
        this(id, attempt,  LocalDateTime.now(), new ArrayList<>());
    }

    private Request(RequestId id, int attempt, LocalDateTime created, List<ActorRef> returnPath) {
        this.id = id;
        this.attempt = attempt;
        this.created = created;
        this.returnPath = returnPath;
    }

    public Request goDownstream(int attempt) {
        var returnPath = new ArrayList<ActorRef>();
        returnPath.addAll(this.returnPath);
        return new Request(this.id, attempt, this.created, returnPath);
    }

    public final RequestId id;
    public final LocalDateTime created;
    public final int attempt;
    public List<ActorRef> returnPath;
}
