package com.unifier;

import com.common.mapreduce.ReducerUserModCommand;
import com.google.common.collect.ImmutableMap;
import com.unifier.facebookprovider.FacebookConvertor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class UnifierUserSegmentsMapperTestCase {

    private UnifierUserSegmentsMapper createInstance(FacebookConvertor convertor) {
        return new UnifierUserSegmentsMapper() {
            @Override
            protected FacebookConvertor getConvertor() {
                return convertor;
            }

            @Override
            protected String getProviderTypeName() {
                return "unifier";
            }
        };
    }

    @Test
    public void testFacebookUserSegmentsMapper() throws IOException, InterruptedException {
        //prepare stage
        Mapper.Context context = mock(Mapper.Context.class);
        Counter mapRedCounter = mock(Counter.class);
        FacebookConvertor convertor = mock(FacebookConvertor.class);
        String timestamp = "2011-12-03T10:15:30+01:00";

        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/rumcSchema.avsc"));

        HashMap<String, String> segmentTimestampsMap = new HashMap<>(ImmutableMap.of("link_clicked", timestamp, "link_hovered", timestamp));
        ReducerUserModCommand userModCommand = new ReducerUserModCommand("15", "add",
                segmentTimestampsMap);
        GenericRecord recordAdd = new GenericData.Record(schema);
        recordAdd.put("userid", userModCommand.getUserId());
        recordAdd.put("command", userModCommand.getCommand());
        recordAdd.put("segmenttimestamps", segmentTimestampsMap);

        HashMap<String, String> segmentTimestampsMap1 = new HashMap<>(ImmutableMap.of("page_closed", timestamp, "page_opened", timestamp));
        ReducerUserModCommand userModCommand1 = new ReducerUserModCommand("15", "delete",
                segmentTimestampsMap);
        GenericRecord recordDelete = new GenericData.Record(schema);
        recordDelete.put("userid", userModCommand.getUserId());
        recordDelete.put("command", userModCommand.getCommand());
        recordDelete.put("segmenttimestamps", segmentTimestampsMap);

        List<ReducerUserModCommand> cmdList = Arrays.asList(userModCommand, userModCommand1);

        String input = "11/link_clicked,link_hovered/page_closed,page_opened";
        UnifierUserSegmentsMapper testMapper = createInstance(convertor);
        when(context.getCounter("unifier.provider_reader", "unifier.map_counter")).thenReturn(mapRedCounter);
        when(convertor.convert(new AbstractMap.SimpleEntry<String, Mapper<org.apache.hadoop.io.LongWritable, org.apache.hadoop.io.Text, java.lang.Void, org.apache.avro.generic.GenericRecord>.Context>(
                input, context))).thenReturn(cmdList);

        //act stage
        testMapper.map(null, new Text(input), context);

        //asserts stage
        verify(mapRedCounter, times(1)).increment(1);
        verify(context, times(1)).write(null, recordAdd);
        verify(context, times(1)).write(null, recordDelete);

    }
}

