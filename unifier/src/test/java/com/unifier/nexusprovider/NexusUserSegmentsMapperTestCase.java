package com.unifier.nexusprovider;

import com.common.mapreduce.ReducerUserModCommand;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.mockito.Mockito.*;

public class NexusUserSegmentsMapperTestCase {

    private NexusUserSegmentsMapper createInstance(
            NexusConvertor convertor) {
        return new NexusUserSegmentsMapper() {

            @Override
            protected NexusConvertor getConvertor() {
                return convertor;
            }
        };
    }

    @Test
    public void testNexusUserSegmentsMapper() throws IOException, InterruptedException {
        //prepare stage
        Context context = mock(Context.class);
        Counter mapRedCounter = mock(Counter.class);
        NexusConvertor convertor = new NexusConvertor();
        String timestamp = "2011-12-03T10:15:30+01:00";

        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/rumcSchema.avsc"));
        HashMap<String, String> segmentTimestampsMap = new HashMap<>(ImmutableMap.of("link_clicked", timestamp, "link_hovered", timestamp));
        ReducerUserModCommand userModCommand = new ReducerUserModCommand("15", "add",
                segmentTimestampsMap);
        GenericRecord recordAdd = new GenericData.Record(schema);
        recordAdd.put("userid", userModCommand.getUserId());
        recordAdd.put("command", userModCommand.getCommand());
        recordAdd.put("segmenttimestamps", segmentTimestampsMap);

        String input = "2011-12-03T10:15:30+01:00,15,add,link_clicked,link_hovered";
        NexusUserSegmentsMapper testMapper = createInstance(convertor);
        when(context.getCounter("nexus_provider_reader", "nexus_map_counter")).thenReturn(mapRedCounter);

        //act stage
        testMapper.map(null, new Text(input), context);

        //asserts stage
        verify(mapRedCounter, times(1)).increment(1);
        verify(context, times(1)).write(null, recordAdd);
    }
}
