package com.segmentreader.mapreduce;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private AbstractUserSegmentsMapper createInstance(Map<String, OperationHandler> handlers,
            List<Closeable> closeables, Convertor convertor) {
        return new AbstractUserSegmentsMapper() {
            @Override
            protected Map<String, OperationHandler> getHandlers() {
                return handlers;
            }

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
    public void testMapperWithValidArgs() throws IOException, InterruptedException, ParseException{
        //prepare stage
        OperationHandler handler = mock(OperationHandler.class);
        Convertor convertor = mock(Convertor.class);
        Context context = mock(Context.class);
        Counter mapRedCounter = mock(Counter.class);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS+hh:mm");
        Date parsedTimeStamp = dateFormat.parse("2014-08-22 15:02:51:580+12:15");
        Timestamp timestamp = new Timestamp(parsedTimeStamp.getTime());
        UserModCommand userMod = new UserModCommand(timestamp, "user22", "delete", Arrays.asList("iphone"));
        Map<String, OperationHandler> handlers = ImmutableMap.of("delete", handler);
        
        String input = "2014-08-22 15:02:51:580+12:15, user22,delete,iphone";
        AbstractUserSegmentsMapper testMapper = createInstance(handlers, null, convertor);
        when(context.getCounter("segmentreader", "mycounter")).thenReturn(mapRedCounter);
        when(convertor.convert(input)).thenReturn(userMod);
        
        //act stage
        testMapper.map(new LongWritable(1), new Text(input), context);
        
        //asserts stage
        verify(convertor, times(1)).convert(input);
        verify(handler, times(1)).handle(userMod);
        verify(mapRedCounter, times(1)).increment(1);
    }
    
    @Test(expected=IOException.class)
    public void testMapperWithInvalidArgs() throws IOException, InterruptedException, InvalidArgumentException, ParseException{
        //prepare stage
        OperationHandler handler = mock(OperationHandler.class);
        Convertor convertor = mock(Convertor.class);
        Context context = mock(Context.class);
        Map<String, OperationHandler> handlers = ImmutableMap.of("delete", handler);
        String input = "2014-08-22 15:02:51:580+12:15,user22,delete"; //not specifying segments in order to invoke error
        doThrow(IOException.class).when(convertor).convert(anyString());
        AbstractUserSegmentsMapper testMapper = createInstance(handlers, null, convertor);
        
        //act stage
        testMapper.map(new LongWritable(1), new Text(input), context);
    }

    @Test
    public void testMapperCleanup() throws IOException, InterruptedException {
        OperationHandler handler = mock(OperationHandler.class);
        Convertor convertor = mock(Convertor.class);
        Context context = mock(Context.class);
        List<Closeable> closeables = Arrays.asList(mock (Closeable.class));
        AbstractUserSegmentsMapper testMapper = createInstance(new HashMap(), closeables, convertor);
        
        testMapper.cleanup(context);
        
        verify(closeables.get(0), times(1)).close();
    }
}
