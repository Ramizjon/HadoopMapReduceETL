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
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if ((o == null) || !(o instanceof MapperUserModCommand)) {
            return false;
        }

        MapperUserModCommand temp = (MapperUserModCommand) o;

        if (!temp.getUserId().equals(this.getUserId())
                || !temp.getCommand().equals(this.getCommand())
                || !temp.getSegments().equals(this.getSegments())
                || !temp.getTimestamp().equals(this.getTimestamp())) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int stPoint = 14;
        int result = 1;
        result = result * stPoint + getCommand().hashCode();
        result = result * stPoint + getUserId().hashCode();
        result = result * stPoint + getSegments().hashCode();
        result = result * stPoint + getTimestamp().hashCode();
        return result;
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
