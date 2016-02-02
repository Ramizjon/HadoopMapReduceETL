package com.segmentreader.domain;

import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

public class User {
    String userId;
    List<String> segments;
    Timestamp timestamp;

    public User(Timestamp timestamp, String userId, List<String> segments) {
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

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

}
