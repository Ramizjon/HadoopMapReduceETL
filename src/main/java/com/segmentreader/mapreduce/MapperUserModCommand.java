package com.segmentreader.mapreduce;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import lombok.Data;
import lombok.ToString;

import java.io.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

@Data
@ToString
public class MapperUserModCommand implements Serializable, Comparable<MapperUserModCommand> {
    Instant timestamp;
    String userId;
    String command;
    ArrayList<String> segments;


    public MapperUserModCommand(Instant timestamp, String userId, String command, ArrayList<String> segments) {
        this.userId = userId;
        this.command = command;
        this.segments = segments;
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(MapperUserModCommand inputUmc) {
        return ComparisonChain.start()
                .compare(getTimestamp(), inputUmc.getTimestamp())
                .compare(getUserId(), inputUmc.getUserId())
                .compare(getCommand(), inputUmc.getCommand())
                .compare(getSegments(), inputUmc.getSegments(),
                        Ordering.<String>natural().lexicographical())
                .result();
    }
}
