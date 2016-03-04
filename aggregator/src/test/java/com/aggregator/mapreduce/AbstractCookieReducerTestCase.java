package com.aggregator.mapreduce;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

import com.aggregator.utils.UserModContainer;
import com.common.mapreduce.MapperUserModCommand;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.aggregator.useroperations.OperationHandler;
import org.mockito.ArgumentCaptor;

public class AbstractCookieReducerTestCase {


    private AbstractCookieReducer createInstance (Map<String, OperationHandler> handlers) {
        
        return new AbstractCookieReducer() {
            @Override
            protected Map<String, OperationHandler> getHandlers() {
               return handlers;
            }
        };
    }
    
    @Test
    public void testAbstractCookieReducer() throws IOException, InterruptedException {
        LinkedList lasd = new LinkedList();
        OperationHandler handler = mock(OperationHandler.class);
        String timestampValue = Instant.EPOCH.toString();
        ArgumentCaptor<ReducerUserModCommand> umcCaptor = ArgumentCaptor.forClass(ReducerUserModCommand.class);
        Counter mapRedCounter = mock(Counter.class);
        String timeStamp = Instant.EPOCH.toString();
        Map<String, OperationHandler> handlers = ImmutableMap.of(OperationHandler.DELETE_OPERATION, handler,
                OperationHandler.ADD_OPERATION, handler);
        ArrayList<String> umc1Segments = new ArrayList<>(Arrays.asList("iphone", "macbook", "magic mouse"));
        Collections.reverse(umc1Segments);
        ArrayList<String> umc2Segments = new ArrayList<>(Arrays.asList("dakine bag", "dakine case", "dakine gloves"));
        Collections.reverse(umc2Segments);
        ArrayList<String> umc3Segments = new ArrayList<>(Arrays.asList("page closed", "page opened"));
        Collections.reverse(umc3Segments);
        ArrayList<String> umc4Segments = new ArrayList<>(Arrays.asList("link highlighted", "link copied"));
        Collections.reverse(umc4Segments);
        UserModContainer<MapperUserModCommand> umc = new UserModContainer<>(new MapperUserModCommand(timeStamp,"11",OperationHandler.ADD_OPERATION,umc1Segments));
        UserModContainer<MapperUserModCommand> umc1 = new UserModContainer<>(new MapperUserModCommand(timeStamp,"11",OperationHandler.DELETE_OPERATION, umc2Segments));
        UserModContainer<MapperUserModCommand> umc3 = new UserModContainer<>(new MapperUserModCommand(timeStamp,"11",OperationHandler.DELETE_OPERATION, umc3Segments));
        UserModContainer<MapperUserModCommand> umc4 = new UserModContainer<>(new MapperUserModCommand(timeStamp,"11",OperationHandler.ADD_OPERATION, umc4Segments));
        Iterable<UserModContainer<MapperUserModCommand>> values = Arrays.asList(umc,umc1,umc3,umc4);
        Context context = mock(Context.class);
        
        when(context.getCounter("aggregator", "reduce_counter")).thenReturn(mapRedCounter);
        
        AbstractCookieReducer reducer = createInstance(handlers);
        reducer.reduce(new Text("11"), values, context);

        verify(handler, times(2)).handle(umcCaptor.capture());
        verify(mapRedCounter, times(2)).increment(1);

        //aggregated items
        Map<String, String> map11 = ImmutableMap.of("magic mouse", timestampValue, "link copied", timestampValue,
                "macbook", timestampValue, "iphone", timestampValue, "link highlighted", timestampValue);
        ReducerUserModCommand expectedUmc1 = new ReducerUserModCommand("11", OperationHandler.ADD_OPERATION, map11);

        Map<String, String> map22 = ImmutableMap.of("page closed", timestampValue, "dakine bag", timestampValue,
                "page opened", timestampValue, "dakine case", timestampValue, "dakine gloves", timestampValue);
        ReducerUserModCommand expectedUmc2 = new ReducerUserModCommand("11", OperationHandler.DELETE_OPERATION, map22);

        List<ReducerUserModCommand> expectedUmcList = new LinkedList<>();
        expectedUmcList.add(expectedUmc1);
        expectedUmcList.add(expectedUmc2);

        assertThat(expectedUmcList, is(umcCaptor.getAllValues()));
    }

}