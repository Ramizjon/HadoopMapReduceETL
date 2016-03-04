package com.aggregator.mapreduce;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;

import com.common.mapreduce.MapperUserModCommand;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.aggregator.useroperations.OperationHandler;

public class AbstractUserSegmentsMapperTestCase {

    private AbstractUserSegmentsMapper createInstance(
            List<Closeable> closeables) {
        return new AbstractUserSegmentsMapper() {
 
            @Override
            protected List<Closeable> getCloseables() {
                return closeables;
            }

        };
    }

    @Test
    public void testMapperWithValidArgs() throws IOException, InterruptedException{
        //prepare stage
        OperationHandler handler = mock(OperationHandler.class);
        GenericRecord genericRecord = mock(GenericRecord.class);
        Context context = mock(Context.class);
        Counter mapRedCounter = mock(Counter.class);
        String timestamp = "2011-12-03T10:15:30+01:00";
        MapperUserModCommand userMod = new MapperUserModCommand(timestamp, "user22", "delete", new ArrayList<>(Arrays.asList("iphone")));
        
        String input = "2011-12-03T10:15:30+01:00,user22,delete,iphone";
        AbstractUserSegmentsMapper testMapper = createInstance(null);
        when(context.getCounter("aggregator", "mapcounter")).thenReturn(mapRedCounter);

        //act stage
        testMapper.map(null, genericRecord, context);

        //asserts stage
        verify(mapRedCounter, times(1)).increment(1);
    }
    

    @Test
    public void testMapperCleanup() throws IOException, InterruptedException {
        Context context = mock(Context.class);
        List<Closeable> closeables = Arrays.asList(mock (Closeable.class));
        AbstractUserSegmentsMapper testMapper = createInstance( closeables);
        
        testMapper.cleanup(context);
        
        verify(closeables.get(0), times(1)).close();
    }
}
