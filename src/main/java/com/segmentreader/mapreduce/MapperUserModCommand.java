package com.segmentreader.mapreduce;

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
        int result = getTimestamp().compareTo(inputUmc.getTimestamp());
        if (result == 0){
            result += ((inputUmc.getSegments().size() == getSegments().size())
                    && inputUmc.getSegments().containsAll(getSegments())
                    && (getUserId().equals(inputUmc.getUserId()))
                    &&(getCommand().equals(inputUmc.getCommand()))) ? 0 : 1;
        }
        return result;
    }
}
