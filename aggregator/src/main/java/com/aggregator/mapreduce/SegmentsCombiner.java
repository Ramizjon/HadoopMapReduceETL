package com.aggregator.mapreduce;

import com.aggregator.useroperations.OperationHandler;
import com.aggregator.utils.UserModContainer;
import com.common.mapreduce.ReducerUserModCommand;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
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
public class SegmentsCombiner extends AbstractReducer <Text, UserModContainer<ReducerUserModCommand>> {

    protected void writeToContext(Map<String, String> readyMap, String command, Text key, Context context) {
        ReducerUserModCommand rumc = new ReducerUserModCommand(key.toString(), command, readyMap);
        try {
            context.write(new Text(rumc.getUserId()), new UserModContainer<>(rumc));
            context.getCounter(appName, reduceCounter).increment(1);
        } catch (IOException | InterruptedException e) {
            log.error("Exception occured. Arguments: {}, exception code: {}", key.toString(), e);
            context.getCounter(appName, errorCounter).increment(1);
        }
    }

    @Override
    protected String getReducerType() {
        return "combiner";
    }

}