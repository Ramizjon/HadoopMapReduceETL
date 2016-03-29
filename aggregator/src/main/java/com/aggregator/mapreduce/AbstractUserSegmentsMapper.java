package com.aggregator.mapreduce;

import com.aggregator.utils.UserModContainer;
import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.common.mapreduce.*;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;

import com.common.mapreduce.ReducerUserModCommand;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public abstract class AbstractUserSegmentsMapper extends
        Mapper<Void, GenericRecord, Text, UserModContainer<ReducerUserModCommand>> {

    private static final String mapCounter = "mapcounter";
    private static final String errorCounter = "map_error_counter";
    private static final String groupName = "aggregator";

    public void map(Void key, GenericRecord value, Context context)
            throws IOException, InterruptedException {
        log.debug("Map job started");
        ReducerUserModCommand rumc = null;
        try {
            HashMap<String, String> genericMap = (HashMap<String, String>) value.get("segmenttimestamps");

            rumc = new ReducerUserModCommand((String) value.get("userid"), (String) value.get("command"),
                    genericMap);
            UserModContainer<ReducerUserModCommand> umc = new UserModContainer<>(rumc);

            context.write(new Text(umc.getData().getUserId()), umc);
            context.getCounter(groupName, mapCounter).increment(1);
        } catch (InvalidArgumentException e) {
            log.error("Exception occured. Arguments: {}, exception code: {}", value.toString(), e);
            context.getCounter(groupName, errorCounter).increment(1);
        }
    }
}