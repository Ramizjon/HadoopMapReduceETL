package com.aggregator.mapreduce;

import com.aggregator.useroperations.OperationHandler;
import com.aggregator.utils.UserModContainer;
import com.common.mapreduce.MapperUserModCommand;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;

public class AbstractUserSegmentsMapperTestCase {

    private AbstractUserSegmentsMapper createInstance() {
        return new AbstractUserSegmentsMapper(){};
    }

    @Test
    public void testMapperWithValidArgs() throws IOException, InterruptedException{
        //prepare stage
        OperationHandler handler = mock(OperationHandler.class);
        Context context = mock(Context.class);
        Counter mapRedCounter = mock(Counter.class);
        String timestamp = "2011-12-03T10:15:30+01:00";

        Schema familyArraySchema = Schema.createArray(ReflectData.get().getSchema(String.class));
        Array<String> segmentsArray = new Array<String>(familyArraySchema, Arrays.asList("opened_browser", "closed_browser"));
        MapperUserModCommand userModCommand = new MapperUserModCommand(timestamp, "15", "delete",
                new ArrayList<>(Arrays.asList("opened_browser", "closed_browser")));

        Schema schema = ReflectData.get().getSchema(MapperUserModCommand.class);
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("timestamp", userModCommand.getTimestamp());
        genericRecord.put("userId", userModCommand.getUserId());
        genericRecord.put("command", userModCommand.getCommand());
        genericRecord.put("segments", segmentsArray);

        AbstractUserSegmentsMapper testMapper = createInstance();
        when(context.getCounter("aggregator", "mapcounter")).thenReturn(mapRedCounter);

        //act stage
        testMapper.map(null, genericRecord, context);

        //asserts stage
        verify(mapRedCounter, times(1)).increment(1);
        verify(context, times(1)).write(new Text(userModCommand.getUserId()),
                new UserModContainer<MapperUserModCommand>(userModCommand));
    }
    


}
