package com.aggregator.mapreduce;

import com.aggregator.useroperations.OperationHandler;
import com.aggregator.utils.UserModContainer;
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
import java.util.*;

import static org.mockito.Mockito.*;

public class AbstractUserSegmentsMapperTestCase {

    private AbstractUserSegmentsMapper createInstance() {
        return new AbstractUserSegmentsMapper(){};
    }

    @Test
    public void testMapper() throws IOException, InterruptedException{
        //prepare stage
        OperationHandler handler = mock(OperationHandler.class);
        Context context = mock(Context.class);
        Counter mapRedCounter = mock(Counter.class);
        String timestamp = "2011-12-03T10:15:30+01:00";

//      Schema familyArraySchema = Schema.createArray(ReflectData.get().getSchema(String.class));
//      Array<String> segmentsArray = new Array<String>(familyArraySchema, Arrays.asList("opened_browser", "closed_browser"));
        HashMap<String, String> segmentTimestampsMap = new HashMap<>(ImmutableMap.of("opened_browser", timestamp, "closed_browser", timestamp));

        ReducerUserModCommand userModCommand = new ReducerUserModCommand("15", "delete",
                ImmutableMap.of("opened_browser", timestamp, "closed_browser", timestamp));

        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/rumcSchema.avsc"));
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("userid", userModCommand.getUserId());
        genericRecord.put("command", userModCommand.getCommand());
        genericRecord.put("segmenttimestamps", segmentTimestampsMap);

        AbstractUserSegmentsMapper testMapper = createInstance();
        when(context.getCounter("aggregator", "mapcounter")).thenReturn(mapRedCounter);

        //act stage
        testMapper.map(null, genericRecord, context);

        //asserts stage
        verify(mapRedCounter, times(1)).increment(1);
        verify(context, times(1)).write(new Text(userModCommand.getUserId()),
                new UserModContainer<ReducerUserModCommand>(userModCommand));
    }
    


}
