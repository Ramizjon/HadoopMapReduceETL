package com.aggregator.mapreduce;


import com.aggregator.utils.UserModContainer;
import com.common.mapreduce.MapperUserModCommand;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.hbase.util.Triple;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Combiner logic is similar to Reducer one, though all methods are different.
 * That's the reason they don't extend common ancestor
 */
@Slf4j
public class SegmentsCombiner extends
        Reducer<Text, UserModContainer<MapperUserModCommand>, Text, UserModContainer<MapperUserModCommand>> {

    private static final String combinerCounter = "combine_counter";
    private static final String errorCounter = "combine_error_counter";
    private static final String appName = "aggregator";

    @Override
    public void reduce(Text key, Iterable<UserModContainer<MapperUserModCommand>> values, Context context)
            throws IOException, InterruptedException {

        List<MapperUserModCommand> userModList = new ArrayList<>();
        values.forEach(e -> userModList.add(e.getData()));

        log.info("Combiner has received: {}", userModList);

        userModList.stream()
                .filter(p -> !p.getSegments().isEmpty())
                .collect(Collectors.groupingBy(MapperUserModCommand::getCommand))
                .entrySet()
                .stream()
                .map(this::getUmcTriple)
                .forEach(e -> {
                    writeToContext(e.getFirst(), e.getSecond(), e.getThird(), key, context);
                });
        cleanup(context);
    }

    private Triple<String, String, List<String>> getUmcTriple(Map.Entry<String, List<MapperUserModCommand>> e) {
        String timestamp = e.getValue().get(0).getTimestamp();
        String command = e.getKey();
        List<String> segmentsList = e.getValue()
                .stream()
                .flatMap(mapperUserModCommand -> {
                    return mapperUserModCommand.getSegments()
                            .stream().distinct();
                }).collect(Collectors.toList());
        return new Triple<>(timestamp, command, segmentsList);
    }

    protected void writeToContext(String timestamp, String command, List<String> segmentsList, Text key, Context context) {
        UserModContainer<MapperUserModCommand> umc = new UserModContainer<>(new MapperUserModCommand(timestamp, key.toString(),
                command, Lists.newArrayList(segmentsList)));
        try {
            Schema schema = ReflectData.get().getSchema(MapperUserModCommand.class);
            context.write(key, umc);
            context.getCounter(appName, combinerCounter).increment(1);
        } catch (IOException | InterruptedException e) {
            log.error("Exception occured. Arguments: {}, exception code: {}", key.toString(), e);
            context.getCounter(appName, errorCounter).increment(1);
        }
    }

}