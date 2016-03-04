package com.aggregator.domain;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;

@Data
@Slf4j
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


}
