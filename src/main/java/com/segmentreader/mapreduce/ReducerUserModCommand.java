package com.segmentreader.mapreduce;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.time.Instant;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;

@Data
@AllArgsConstructor
public class ReducerUserModCommand implements Serializable, Comparable <ReducerUserModCommand>{
    String userId;
    String command;
    SimpleEntry <ArrayList<String>, Instant> segmentTimestamps;

    @Override
    public int compareTo(ReducerUserModCommand inputUmc) {
        int result = segmentTimestamps.getValue().compareTo(inputUmc.segmentTimestamps.getValue());
        if (result == 0){
            result += ((inputUmc.segmentTimestamps.getKey().size() == segmentTimestamps.getKey().size())
                    && inputUmc.segmentTimestamps.getKey().containsAll(segmentTimestamps.getKey())
                    && (getUserId().equals(inputUmc.getUserId()))
                    &&(getCommand().equals(inputUmc.getCommand()))) ? 0 : 1;
        }
        return result;
    }
}
