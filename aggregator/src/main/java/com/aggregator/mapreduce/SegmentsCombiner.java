package com.aggregator.mapreduce;

import com.aggregator.useroperations.OperationHandler;
import com.aggregator.utils.UserModContainer;
import com.common.mapreduce.ReducerUserModCommand;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.Closeable;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class SegmentsCombiner extends
        Reducer<Text, UserModContainer<ReducerUserModCommand>, Text, UserModContainer<ReducerUserModCommand>> {

    private static final String combineCounter = "combine_counter";
    private static final String errorCounter = "combine_error_counter";
    private static final String appName = "aggregator";

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

    private SimpleEntry<String, List<SimpleEntry<String, String>>> getSimpleEntry(Map.Entry<String, List<ReducerUserModCommand>> e) {
        List<SimpleEntry<String, String>> readyMap = e.getValue()
                .stream()
                .flatMap(reducerUserModCommand -> {
                    return reducerUserModCommand.getSegmentTimestamps()
                            .entrySet()
                            .stream().distinct()
                            .map(s -> new SimpleEntry<>(s.getKey(), s.getValue()));
                }).collect(Collectors.toList());

        return new SimpleEntry<>(e.getKey(), readyMap);
    }

    private void writeToContext(Map<String, String> readyMap, String command, Text key, Context context) {
        ReducerUserModCommand rumc = new ReducerUserModCommand(key.toString(), command, readyMap);
        try {
            context.write(new Text(rumc.getUserId()), new UserModContainer<ReducerUserModCommand>(rumc));
            context.getCounter(appName, combineCounter).increment(1);
        } catch (IOException | InterruptedException e) {
            log.error("Exception occured. Arguments: {}, exception code: {}", key.toString(), e);
            context.getCounter(appName, errorCounter).increment(1);
        }
    }

}