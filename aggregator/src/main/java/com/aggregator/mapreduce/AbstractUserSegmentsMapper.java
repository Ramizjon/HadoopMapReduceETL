package com.aggregator.mapreduce;

import com.aggregator.utils.UserModContainer;
import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.common.mapreduce.MapperUserModCommand;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public abstract class AbstractUserSegmentsMapper extends
        Mapper<Void, GenericRecord, Text, UserModContainer<MapperUserModCommand>> {

    private static final String mapCounter = "mapcounter";
    private static final String errorCounter = "map_error_counter";
    private static final String groupName = "aggregator";

    // class dependencies

    public void map(Void key, GenericRecord value, Context context)
            throws IOException, InterruptedException {
        log.debug("Map job started");
        MapperUserModCommand cmd = null;
        try {
            Array<String> genericArray = (Array<String>) value.get("segments");
            log.info("Array contents: {}", genericArray.get(0) );
            ArrayList<String> commandsList = IntStream.range(0, genericArray.size())
                    .mapToObj(i -> genericArray.get(i))
                    .collect(Collectors.toCollection(ArrayList<String>::new));

            cmd = new MapperUserModCommand((String) value.get("timestamp"), (String) value.get("userid"), (String) value.get("command"),
                    commandsList);
            log.info("Mapper has received: {}", cmd.toString());
            UserModContainer<MapperUserModCommand> umc = new UserModContainer<>(cmd);

            context.write(new Text(umc.getData().getUserId()), umc);
            context.getCounter(groupName, mapCounter).increment(1);
        } catch (InvalidArgumentException e) {
            log.error("Exception occured. Arguments: {}, exception code: {}", value.toString(), e);
            context.getCounter(groupName, errorCounter).increment(1);
        }
    }
}