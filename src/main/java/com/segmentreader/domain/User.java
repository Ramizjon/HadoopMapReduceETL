package com.segmentreader.domain;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;

public class User {
    Instant timestamp;
    String userId;
    List<String> segments;

    public User(Instant timestamp, String userId, List<String> segments) {
        super();
        this.timestamp = timestamp;
        this.userId = userId;
        this.segments = segments;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public List<String> getSegments() {
        return segments;
    }

    public void setSegments(LinkedList<String> segment) {
        this.segments = segment;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

}
