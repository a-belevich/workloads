package org.workloads;

public class Response {

    public enum Status {
        Ok,
        Discarded,
        Error,
        DownstreamError,
    }

    public Request request;
    public Status status;
}
