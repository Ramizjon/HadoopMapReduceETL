package com.common.mapreduce;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;
import java.util.ArrayList;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class MapperUserModCommand implements Serializable, Comparable<MapperUserModCommand> {
    String timestamp;
    String userId;
    String command;
    ArrayList<String> segments;

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

