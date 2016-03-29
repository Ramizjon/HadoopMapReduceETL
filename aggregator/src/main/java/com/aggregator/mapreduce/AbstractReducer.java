package com.aggregator.mapreduce;


import com.aggregator.utils.UserModContainer;
import com.common.mapreduce.ReducerUserModCommand;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public abstract class AbstractReducer <K,V> extends
        Reducer<Text, UserModContainer<ReducerUserModCommand>, K, V> {

    protected String reduceCounter = getReducerType().concat(".counter");
    protected String errorCounter = getReducerType().concat(".error_counter");
    protected String appName = "aggregator";

    @Override
    public void reduce(Text key, Iterable<UserModContainer<ReducerUserModCommand>> values, Context context)
            throws IOException, InterruptedException {
        List<ReducerUserModCommand> userModList = new ArrayList<>();
        values.forEach(e -> userModList.add(e.getData()));

        userModList.stream()
                .filter(p -> !p.getSegmentTimestamps().isEmpty())
                .collect(Collectors.groupingBy(ReducerUserModCommand::getCommand))
                .entrySet()
                .stream()
                .map(this::getSimpleEntry)
                .forEach(e -> {
                    Map<String, String> map = new HashMap<>();
                    e.getValue().forEach(p -> {
                        map.put(p.getKey(), p.getValue());
                    });
                    writeToContext(map, e.getKey(), key, context);
                });
        cleanup(context);
    }

    private AbstractMap.SimpleEntry<String, List<AbstractMap.SimpleEntry<String, String>>> getSimpleEntry(Map.Entry<String, List<ReducerUserModCommand>> e) {
        List<AbstractMap.SimpleEntry<String, String>> readyMap = e.getValue()
                .stream()
                .flatMap(reducerUserModCommand -> {
                    return reducerUserModCommand.getSegmentTimestamps()
                            .entrySet()
                            .stream().distinct()
                            .map(s -> new AbstractMap.SimpleEntry<>(s.getKey(), s.getValue()));
                }).collect(Collectors.toList());

        return new AbstractMap.SimpleEntry<>(e.getKey(), readyMap);
    }

    protected abstract void writeToContext(Map<String, String> readyMap, String command, Text key, Context context);
    protected abstract String getReducerType();

}
