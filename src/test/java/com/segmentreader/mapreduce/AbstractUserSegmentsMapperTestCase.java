package com.segmentreader.mapreduce;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;

import com.segmentreader.dataformats.ConvertorImpl;
import com.segmentreader.useroperations.OperationHandler;

public class AbstractUserSegmentsMapperTestCase {

    private AbstractUserSegmentsMapper createInstance(Map<String, OperationHandler> handlers,
            List<Closeable> closeables, ConvertorImpl convertor) {
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
            protected ConvertorImpl getConvertor() {
                return convertor;
            }
        };
    }

    @Test
    public void testMapperWithValidArgs() throws IOException, InterruptedException{
        //prepare stage
        OperationHandler handler = mock(OperationHandler.class);
        ConvertorImpl convertor = mock(ConvertorImpl.class);
        Context context = mock(Context.class);
        Counter mapRedCounter = mock(Counter.class);
        UserModCommand userMod = new UserModCommand("user22", "delete", Arrays.asList("iphone"));
        String input = "user22,delete,iphone";
        AbstractUserSegmentsMapper testMapper = createInstance(new HashMap<String, OperationHandler>(){{
            put("delete",handler);
        }}, null, convertor);
        when(context.getCounter("segmentreader", "mycounter")).thenReturn(mapRedCounter);
        when(convertor.convert(input)).thenReturn(userMod);
        
        //act stage
        testMapper.map(new LongWritable(1), new Text(input), context);
        
        //asserts stage
        verify(convertor, times(1)).convert(input);
        verify(handler, times(1)).handle(userMod);
        verify(mapRedCounter, times(1)).increment(1);
    }
    
    @Test(expected=NullPointerException.class)
    public void testMapperWithInvalidArgs() throws IOException, InterruptedException{
        //prepare stage
        OperationHandler handler = mock(OperationHandler.class);
        ConvertorImpl convertor = new ConvertorImpl();
        Context context = mock(Context.class);
        String input = "user22,delete"; //not specifying segments in order to invoke error
        
        AbstractUserSegmentsMapper testMapper = createInstance(new HashMap<String, OperationHandler>(){{
            put("delete",handler);
        }}, null, convertor);
        
        //act stage
        testMapper.map(new LongWritable(1), new Text(input), context);
    }

    @Test
    public void testMapperCleanup() throws IOException, InterruptedException {
        OperationHandler handler = mock(OperationHandler.class);
        ConvertorImpl convertor = mock(ConvertorImpl.class);
        Context context = mock(Context.class);
        List<Closeable> closeables = Arrays.asList(mock (Closeable.class));
        AbstractUserSegmentsMapper testMapper = createInstance(new HashMap<String, OperationHandler>(){{
            put("delete",handler);
        }}, closeables, convertor);
        
        testMapper.cleanup(context);
        
        verify(closeables.get(0), times(1)).close();
    }
}