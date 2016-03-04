package com.aggregator.mapreduce;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.time.Instant;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@Data
@AllArgsConstructor
public class ReducerUserModCommand implements Serializable, Comparable <ReducerUserModCommand>{
    String userId;
    String command;
    Map<String, String> segmentTimestamps;

    @Override
    public int compareTo(ReducerUserModCommand inputUmc) {
        int result = (segmentTimestamps.size()==inputUmc.getSegmentTimestamps().size())?0:1;
        if (result == 0){
            for (String s: segmentTimestamps.keySet()){
                if (!inputUmc.getSegmentTimestamps().containsKey(s)){
                    return 1;
                }
                else result += segmentTimestamps.get(s)
                        .equals(inputUmc.getSegmentTimestamps().get(s))?0:1;
            }
            result += ((getUserId().equals(inputUmc.getUserId()))
                    &&(getCommand().equals(inputUmc.getCommand())))?0:1;
        }
        return result;
    }
}
