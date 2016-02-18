package com.segmentreader.mapreduce;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.segmentreader.utils.CSVConverter;
import com.segmentreader.utils.ParquetAppender;
import com.segmentreader.utils.UserModContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;
import com.segmentreader.useroperations.OperationHandler;
import parquet.hadoop.ParquetWriter;

@Slf4j
public abstract class AbstractCookieReducer extends
        Reducer<Text, UserModContainer<MapperUserModCommand>, Text, NullWritable> {

    private Map<String, OperationHandler> handlers = getHandlers();
    private ParquetAppender<ParquetCompatibleUserModCommand> parquetAppender;
    private static final String reduceCounter = "reduce_counter";
    private static final String errorCounter = "reduce_error_counter";
    private static final String appName = "segmentreader";

    @Override
    public void reduce(Text key, Iterable<UserModContainer<MapperUserModCommand>> values, Context context)
            throws IOException, InterruptedException {
        if(parquetAppender==null){
            parquetAppender = getParquetAppender(context);
        }
        List <MapperUserModCommand>  userModList =
                Lists.newArrayList(values)
                        .stream()
                        .map(e -> e.getData())
                        .collect(Collectors.toList());

        userModList.forEach(u -> {
            log.info(u.toString());
        });

        userModList.stream()
                .filter(p -> !p.getSegments().isEmpty())
                .collect(Collectors.groupingBy(MapperUserModCommand::getCommand))
                .entrySet().stream()
                .map(e -> ImmutableMap.of(e.getKey(), new SimpleEntry<Set<String>, Instant>(
                    e.getValue().stream()
                        .map(MapperUserModCommand::getSegments)
                        .flatMap(List::stream)
                        .collect(Collectors.toSet()),
                        e.getValue()
                            .stream().max((e1, e2) -> e1.getTimestamp()
                            .compareTo(e2.getTimestamp())).get().getTimestamp())))
                .forEach(e -> { e.forEach((f,s) -> {callHandlers(s, f, key, context); }); });
        
    }

    private void callHandlers (SimpleEntry<Set<String>,Instant> keyValuePair, String command, Text key, Context context) {
        SimpleEntry<ArrayList<String>, Instant> inputEntry
                = new SimpleEntry(new ArrayList<>(keyValuePair.getKey()),keyValuePair.getValue());
        ReducerUserModCommand rumc = new ReducerUserModCommand(key.toString(),command, inputEntry);
            try {
                handlers.get(command).handle(rumc);
                parquetAppender.append(new ParquetCompatibleUserModCommand(rumc));
                context.write(new Text(CSVConverter.convertUserModToCSV(rumc)), NullWritable.get());
                context.getCounter(appName,reduceCounter).increment(1);
            } catch (IOException|InterruptedException e) {
                log.error("Exception occured. Arguments: {}, exception code: {}", key.toString(), e);
                context.getCounter(appName, errorCounter).increment(1);
            }
    }

    @Override
    protected void cleanup(Context context
    ) throws IOException, InterruptedException {
        parquetAppender.close();
    }

    protected abstract Map<String, OperationHandler> getHandlers();
    protected abstract ParquetAppender<ParquetCompatibleUserModCommand> getParquetAppender(Context context);
}