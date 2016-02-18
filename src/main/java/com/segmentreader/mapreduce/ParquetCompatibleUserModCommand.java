package com.segmentreader.mapreduce;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;

@Data
@AllArgsConstructor
public class ParquetCompatibleUserModCommand implements Serializable, Comparable<ParquetCompatibleUserModCommand> {
    String userId;
    String command;
    ArrayList<String> segments;
    String timestamp;

    public ParquetCompatibleUserModCommand(ReducerUserModCommand rumc){
        userId = rumc.getUserId();
        command = rumc.getCommand();
        segments = rumc.getSegmentTimestamps().getKey();
        timestamp = rumc.getSegmentTimestamps().getValue().toString();
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
    public int compareTo(ParquetCompatibleUserModCommand inputUmc) {
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