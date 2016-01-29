package com.segmentreader.mapreduce;

import static org.mockito.Mockito.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;
import org.mockito.Mockito;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;

import com.segmentreader.dataformats.Convertor;
import com.segmentreader.domain.UserRepository;
import com.segmentreader.mapreduce.AbstractUserSegmentsMapper;
import com.segmentreader.mapreduce.AppContext;
import com.segmentreader.mapreduce.UserModCommand;
import com.segmentreader.useroperations.DeleteOperationHandler;
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
    public void testMapperWithValidArgs() throws IOException, InterruptedException{
        //prepare stage
        Map<String, OperationHandler> handlers = mock(Map.class);
        List<Closeable> closeables = mock(List.class);
        OperationHandler handler = mock(OperationHandler.class);
        Convertor convertor = mock(Convertor.class);
        Context context = mock(Context.class);
        Counter mapRedCounter = mock(Counter.class);
        UserModCommand userMod = new UserModCommand("user22", "delete", Arrays.asList("iphone"));
        String input = "user22,delete,iphone";
        
        AbstractUserSegmentsMapper testMapper = createInstance(handlers, closeables, convertor);

        when(handlers.get("delete")).thenReturn(handler);
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
    public void testMapperWithInvalidArgs() throws IOException, InterruptedException{
        //prepare stage
        Map<String, OperationHandler> handlers = mock(Map.class);
        List<Closeable> closeables = mock(List.class);
        OperationHandler handler = mock(OperationHandler.class);
        Convertor convertor = new Convertor();
        Context context = mock(Context.class);
        Counter mapRedCounter = mock(Counter.class);
        UserModCommand userMod = new UserModCommand("user22", "delete", Arrays.asList("iphone"));
        String input = "user22,delete"; //not specifying segments in order to invoke error
        
        AbstractUserSegmentsMapper testMapper = createInstance(handlers, closeables, convertor);

        when(handlers.get("delete")).thenReturn(handler);
        when(context.getCounter("segmentreader", "mycounter")).thenReturn(mapRedCounter);
        when(convertor.convert(input)).thenReturn(userMod);
        
        //act stage
        testMapper.map(new LongWritable(1), new Text(input), context);

    }

}
