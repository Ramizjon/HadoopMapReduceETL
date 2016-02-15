package com.segmentreader.mapreduce;

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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.google.common.collect.ImmutableMap;
import com.segmentreader.dataformats.Convertor;
import com.segmentreader.useroperations.OperationHandler;

public class AbstractUserSegmentsMapperTestCase {

    private AbstractUserSegmentsMapper createInstance(
            List<Closeable> closeables, Convertor convertor) {
        return new AbstractUserSegmentsMapper() {
 
            @Override
            protected List<Closeable> getCloseables() {
                return closeables;
            }

            @Override
            protected Convertor getConvertor() {
                return convertor;
            }
        };
    }

    @Test
    public void testMapperWithValidArgs() throws IOException, InterruptedException{
        //prepare stage
        OperationHandler handler = mock(OperationHandler.class);
        Convertor convertor = mock(Convertor.class);
        Context context = mock(Context.class);
        Counter mapRedCounter = mock(Counter.class);
        Instant timestamp = Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse("2011-12-03T10:15:30+01:00"));
        UserModCommand userMod = new UserModCommand(timestamp, "user22", "delete", new ArrayList<>(Arrays.asList("iphone")));
        
        String input = "2011-12-03T10:15:30+01:00,user22,delete,iphone";
        AbstractUserSegmentsMapper testMapper = createInstance( null, convertor);
        when(context.getCounter("segmentreader", "mapcounter")).thenReturn(mapRedCounter);
        when(convertor.convert(input)).thenReturn(userMod);
        
        //act stage
        testMapper.map(new LongWritable(1), new Text(input), context);
        
        //asserts stage
        verify(convertor, times(1)).convert(input);
        verify(mapRedCounter, times(1)).increment(1);
    }
    
    @Test(expected=IOException.class)
    public void testMapperWithInvalidArgs() throws IOException, InterruptedException, InvalidArgumentException{
        //prepare stage
        Convertor convertor = mock(Convertor.class);
        Context context = mock(Context.class);
        String input = "2011-12-03T10:15:30+01:00,user22,delete"; //not specifying segments in order to invoke error
        doThrow(IOException.class).when(convertor).convert(anyString());
        AbstractUserSegmentsMapper testMapper = createInstance( null, convertor);
        
        //act stage
        testMapper.map(new LongWritable(1), new Text(input), context);
    }

    @Test
    public void testMapperCleanup() throws IOException, InterruptedException {
        Convertor convertor = mock(Convertor.class);
        Context context = mock(Context.class);
        List<Closeable> closeables = Arrays.asList(mock (Closeable.class));
        AbstractUserSegmentsMapper testMapper = createInstance( closeables, convertor);
        
        testMapper.cleanup(context);
        
        verify(closeables.get(0), times(1)).close();
    }
}
