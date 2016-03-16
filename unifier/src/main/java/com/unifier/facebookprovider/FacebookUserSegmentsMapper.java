package com.unifier.facebookprovider;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
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
import utils.Convertor;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;


@Slf4j
public abstract class FacebookUserSegmentsMapper extends
            Mapper<LongWritable, Text, Void, GenericRecord> {

        private static final String mapCounter = "facebook_map_counter";
        private static final String errorCounter = "facebook_map_error_counter";
        private static final String groupName = "facebook_provider_reader";

        private FacebookConvertor convertor = getConvertor();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            log.debug("Map job started");
            List<ReducerUserModCommand> cmdList = null;
            try{
                cmdList = convertor.convert(new SimpleEntry<String, Context>(value.toString(), context));
                Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/rumcSchema.avsc"));
                cmdList.forEach(e -> {
                    GenericRecord record = new GenericData.Record(schema);
                    record.put("userid", e.getUserId());
                    record.put("command", e.getCommand());
                    record.put("segmenttimestamps", e.getSegmentTimestamps());
                    try {
                        context.write(null, record);
                    } catch (IOException |InterruptedException e1) {
                        throw new RuntimeException(e1);
                    }
                });

                context.getCounter(groupName, mapCounter).increment(1);
            } catch (InvalidArgumentException e) {
                log.error("Exception occured. Arguments: {}, exception code: {}", value.toString(), e);
                context.getCounter(groupName, errorCounter).increment(1);
            }
        }

        protected abstract FacebookConvertor getConvertor();

        @Override
        public String toString() {
            return "facebook";
        }

    }
