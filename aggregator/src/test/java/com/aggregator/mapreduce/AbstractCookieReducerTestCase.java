package com.aggregator.mapreduce;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.*;

import com.aggregator.utils.UserModContainer;
import com.common.mapreduce.MapperUserModCommand;
import com.common.mapreduce.ReducerUserModCommand;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.aggregator.useroperations.OperationHandler;
import org.mockito.ArgumentCaptor;

public class AbstractCookieReducerTestCase {


    private AbstractCookieReducer createInstance (Map<String, OperationHandler> handlers, List<Closeable> closeables) {
        return new AbstractCookieReducer() {
            @Override
            protected List<Closeable> getCloseables() {
                return closeables;
            }
            @Override
            protected Map<String, OperationHandler> getHandlers() {
               return handlers;
            }
        };
    }
    
    @Test
    public void testAbstractCookieReducer() throws IOException, InterruptedException {
        OperationHandler handler = mock(OperationHandler.class);
        List<Closeable> closeables = Arrays.asList(mock (Closeable.class));
        String timestampValue = Instant.EPOCH.toString();
        ArgumentCaptor<ReducerUserModCommand> umcCaptor = ArgumentCaptor.forClass(ReducerUserModCommand.class);
        Counter mapRedCounter = mock(Counter.class);
        String timeStamp = Instant.EPOCH.toString();
        Map<String, OperationHandler> handlers = ImmutableMap.of(OperationHandler.DELETE_OPERATION, handler,
                OperationHandler.ADD_OPERATION, handler);
        Map<String, String> umc1Segments = ImmutableMap.of("iphone", timestampValue, "macbook", timestampValue,
                "magic mouse", timestampValue);
        Map<String, String> umc2Segments = ImmutableMap.of("dakine bag", timestampValue, "dakine case", timestampValue,
                "dakine gloves", timestampValue);
        Map<String, String> umc3Segments = ImmutableMap.of("page closed", timestampValue, "page opened", timestampValue);
        Map<String, String> umc4Segments = ImmutableMap.of("link highlighted", timestampValue, "link copied", timestampValue);

        UserModContainer<ReducerUserModCommand> umc = new UserModContainer<>(new ReducerUserModCommand("11",OperationHandler.ADD_OPERATION,umc1Segments));
        UserModContainer<ReducerUserModCommand> umc1 = new UserModContainer<>(new ReducerUserModCommand("11",OperationHandler.DELETE_OPERATION, umc2Segments));
        UserModContainer<ReducerUserModCommand> umc3 = new UserModContainer<>(new ReducerUserModCommand("11",OperationHandler.DELETE_OPERATION, umc3Segments));
        UserModContainer<ReducerUserModCommand> umc4 = new UserModContainer<>(new ReducerUserModCommand("11",OperationHandler.ADD_OPERATION, umc4Segments));
        Iterable<UserModContainer<ReducerUserModCommand>> values = Arrays.asList(umc,umc1,umc3,umc4);
        Context context = mock(Context.class);
        
        when(context.getCounter("aggregator", "reduce_counter")).thenReturn(mapRedCounter);
        
        AbstractCookieReducer reducer = createInstance(handlers,closeables);
        reducer.reduce(new Text("11"), values, context);

        verify(handler, times(2)).handle(umcCaptor.capture());
        verify(mapRedCounter, times(2)).increment(1);

        //aggregated items
        Map<String, String> map11 = ImmutableMap.of("iphone", timestampValue, "macbook", timestampValue,
                "magic mouse", timestampValue, "link highlighted", timestampValue, "link copied", timestampValue);
        ReducerUserModCommand expectedUmc1 = new ReducerUserModCommand("11", OperationHandler.ADD_OPERATION, map11);

        Map<String, String> map22 = ImmutableMap.of("dakine bag", timestampValue, "dakine case", timestampValue,
                "dakine gloves", timestampValue, "page closed", timestampValue, "page opened", timestampValue);
        ReducerUserModCommand expectedUmc2 = new ReducerUserModCommand("11", OperationHandler.DELETE_OPERATION, map22);

        List<ReducerUserModCommand> expectedUmcList = new LinkedList<>();
        expectedUmcList.add(expectedUmc1);
        expectedUmcList.add(expectedUmc2);

        assertThat(expectedUmcList, is(umcCaptor.getAllValues()));
        verify(closeables.get(0), times(1)).close();
    }


}