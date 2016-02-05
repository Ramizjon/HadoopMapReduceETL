package com.segmentreader.mapreduce;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;
import com.segmentreader.useroperations.OperationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractCookieReducer extends
        Reducer<Text, UserModCommand, Text, Text> {

    private static final Logger logger = LoggerFactory
            .getLogger(AbstractUserSegmentsMapper.class);
    private Map<String, OperationHandler> handlers = getHandlers();
    private static final String reduceCounter = "reduce_counter";
    private static final String errorCounter = "reduce_error_counter";
    private static final String appName = "segmentreader";
    private static final String addOp = OperationHandler.ADD_OPERATION;
    private static final String deleteOp = OperationHandler.DELETE_OPERATION;

    @Override
    public void reduce( Text key, Iterable<UserModCommand> values, Context context)
            throws IOException, InterruptedException {

        List<UserModCommand> userModList = Lists.newArrayList(values);

        userModList.stream()
            .filter(p -> !p.getSegments().isEmpty())
            .collect(Collectors.groupingBy(UserModCommand::getCommand))
            .entrySet().stream()
            .collect(Collectors.toMap( e -> ImmutableMap.of(e.getKey(), e.getValue().stream()
                            .map(UserModCommand::getSegments).collect(Collectors.toList())
                    .stream().flatMap(List::stream).collect(Collectors.toSet())),
                    k -> k.getValue().stream().sorted((e1, e2) -> e1.getTimestamp()
                            .compareTo(e2.getTimestamp())).findFirst().get().getTimestamp())
            ).forEach((k,v) -> {
                k.forEach( (s, f) -> {
                    UserModCommand cmd = new UserModCommand(v, key.toString(), s, new ArrayList<>(f));
                    try {
                        handlers.get(s).handle(cmd);
                    } catch (IOException e) {
                        logger.error("Exception occured. Arguments: {}, exception code: {}", key.toString(), e);
                        context.getCounter(appName, errorCounter).increment(1);
                    }
                });
        });
        context.getCounter(appName,reduceCounter).increment(1);
    }

     protected abstract Map<String, OperationHandler> getHandlers();
}