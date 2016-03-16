package com.unifier.nexusprovider;

import java.io.IOException;

import com.common.mapreduce.MapperUserModCommand;
import com.common.mapreduce.ReducerUserModCommand;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import utils.Convertor;

@Slf4j
public abstract class NexusUserSegmentsMapper extends
        Mapper<LongWritable, Text, Void, GenericRecord> {

    private static final String mapCounter = "nexus_map_counter";
    private static final String errorCounter = "nexus_map_error_counter";
    private static final String groupName = "nexus_provider_reader";

    private NexusConvertor convertor = getConvertor();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        log.debug("Map job started");
        ReducerUserModCommand cmd = null;
        try{
            cmd = convertor.convert(value.toString());
            Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/rumcSchema.avsc"));
            GenericRecord record = new GenericData.Record(schema);
            record.put("userid", cmd.getUserId());
            record.put("command", cmd.getCommand());
            record.put("segmenttimestamps", cmd.getSegmentTimestamps());
            context.write(null, record);
            context.getCounter(groupName, mapCounter).increment(1);
        } catch (InvalidArgumentException e) {
            log.error("Exception occured. Arguments: {}, exception code: {}", value.toString(), e);
            context.getCounter(groupName, errorCounter).increment(1);
        }
    }

    protected abstract NexusConvertor getConvertor();

    @Override
    public String toString() {
        return "nexus";
    }
}