package com.aggregator.mapreduce;

import com.aggregator.utils.UserModContainer;
import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.common.mapreduce.MapperUserModCommand;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

@Slf4j
public abstract class AbstractUserSegmentsMapper extends
        Mapper<Void, GenericRecord, Text, UserModContainer<MapperUserModCommand>> {

    private static final String mapCounter = "mapcounter";
    private static final String errorCounter = "map_error_counter";
    private static final String appName = "aggregator";

    // class dependencies
    private List<Closeable> closeables = getCloseables();

    public void map(Void key, GenericRecord value, Context context)
            throws IOException, InterruptedException {
        log.debug("Map job started");
        log.info("Mapper has received: {}", value.toString());
        MapperUserModCommand cmd = null;
        try {
            cmd = new MapperUserModCommand((String) value.get("timestamp"), (String) value.get("userId"), (String) value.get("command"),
                    Lists.newArrayList((GenericData.Array<String>) value.get("segments")));
            UserModContainer<MapperUserModCommand> umc = new UserModContainer<>(cmd);

            context.write(new Text(umc.getData().getUserId()),umc);
            context.getCounter(appName, mapCounter).increment(1);
        } catch (InvalidArgumentException e) {
            log.error("Exception occured. Arguments: {}, exception code: {}", value.toString(), e);
            context.getCounter(appName, errorCounter).increment(1);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        for (Closeable closeable : closeables) {
            closeable.close();
        }
        log.debug("Clean up completed");
    }

    protected abstract List<Closeable> getCloseables();

}