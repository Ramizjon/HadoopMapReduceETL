package com.aggregator.mapreduce;

import com.common.mapreduce.MapperUserModCommand;
import com.aggregator.mapreduce.ReducerUserModCommand;
import com.aggregator.useroperations.OperationHandler;
import com.aggregator.utils.UserModContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;



@Slf4j
public abstract class AbstractCookieReducer extends
        Reducer<Text, UserModContainer<MapperUserModCommand>, Void , GenericRecord> {

    private Map<String, OperationHandler> handlers = getHandlers();
    private static final String reduceCounter = "reduce_counter";
    private static final String errorCounter = "reduce_error_counter";
    private static final String appName = "aggregator";

    @Override
    public void reduce(Text key, Iterable<UserModContainer<MapperUserModCommand>> values, Context context)
            throws IOException, InterruptedException {

        List <MapperUserModCommand> userModList = new ArrayList<>();
        values.forEach(e -> userModList.add(e.getData()));

        userModList.stream()
                .filter(p -> !p.getSegments().isEmpty())
                .collect(Collectors.groupingBy(MapperUserModCommand::getCommand))
                .entrySet()
                .stream()
                .map(this::getSimpleEntry)
                .forEach(e -> {
                    Map<String, String> map = new HashMap<>();
                    e.getValue().forEach(p -> {map.put(p.getKey(),p.getValue());});
                    callHandlers(map, e.getKey(), key, context);
                });
    }


    private SimpleEntry<String, List<SimpleEntry<String, String>>> getSimpleEntry(Map.Entry<String, List<MapperUserModCommand>> e) {
        List<SimpleEntry<String, String>> readyMap = e.getValue()
                .stream()
                .flatMap(mapperUserModCommand -> {
                    return mapperUserModCommand.getSegments()
                            .stream().distinct()
                            .map(s -> new SimpleEntry<>(s, mapperUserModCommand.getTimestamp().toString()));
                }).collect(Collectors.toList());

        return new SimpleEntry<>(e.getKey(), readyMap);
    }


    private void callHandlers (Map<String, String> readyMap, String command, Text key, Context context) {
        ReducerUserModCommand rumc = new ReducerUserModCommand(key.toString(), command, readyMap);
            try {
                handlers.get(command).handle(rumc);
                Schema schema = ReflectData.get().getSchema(ReducerUserModCommand.class);
                GenericRecord record = new GenericData.Record(schema);
                record.put("userId", rumc.getUserId());
                record.put("command", rumc.getCommand());
                record.put("segmentTimestamps", rumc.getSegmentTimestamps());
                context.write(null, record);
                context.getCounter(appName,reduceCounter).increment(1);
            } catch (IOException|InterruptedException e) {
                log.error("Exception occured. Arguments: {}, exception code: {}", key.toString(), e);
                context.getCounter(appName, errorCounter).increment(1);
            }
    }

    protected abstract Map<String, OperationHandler> getHandlers();
}