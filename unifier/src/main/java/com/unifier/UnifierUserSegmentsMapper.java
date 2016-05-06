package com.unifier;


import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.common.mapreduce.ReducerUserModCommand;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.Convertor;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;

@Slf4j
public abstract class UnifierUserSegmentsMapper extends
        Mapper<LongWritable, Text, Void, GenericRecord> {

    private String mapCounter = getProviderTypeName().concat(".map_counter");
    private String errorCounter = getProviderTypeName().concat(".map_error_counter");
    private String groupName = getProviderTypeName().concat(".provider_reader");
    private Convertor<AbstractMap.SimpleEntry<String, Context>, List<ReducerUserModCommand>> convertor = getConvertor();
    Schema schema = getSchema();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        log.debug("Map job started");
        List<ReducerUserModCommand> cmdList = null;
        try {
            cmdList = convertor.convert(new AbstractMap.SimpleEntry<String, Context>(value.toString(), context));
            cmdList.forEach(e -> {
                GenericRecord record = new GenericData.Record(schema);
                record.put("userid", e.getUserId());
                record.put("command", e.getCommand());
                record.put("segmenttimestamps", e.getSegmentTimestamps());
                try {
                    context.write(null, record);
                } catch (IOException | InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            });
            context.getCounter(groupName, mapCounter).increment(1);
        } catch (InvalidArgumentException e) {
            log.error("Exception occured. Arguments: {}, exception code: {}", value.toString(), e);
            context.getCounter(groupName, errorCounter).increment(1);
        }
    }

    protected abstract Convertor getConvertor();

    protected abstract String getProviderTypeName();

    @Override
    public String toString() {
        return getProviderTypeName();
    }

    private Schema getSchema() {
        Schema schemaToReturn = null;
        try {
            schemaToReturn = new Schema.Parser().parse(getClass().getResourceAsStream("/rumcSchema.avsc"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return schemaToReturn;
    }
}
