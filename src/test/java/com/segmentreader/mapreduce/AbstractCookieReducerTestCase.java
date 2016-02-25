package com.segmentreader.mapreduce;

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

import com.segmentreader.utils.UserModContainer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.segmentreader.useroperations.OperationHandler;
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
        ArgumentCaptor<ReducerUserModCommand> umcCaptor = ArgumentCaptor.forClass(ReducerUserModCommand.class);
        Counter mapRedCounter = mock(Counter.class);
        Instant timeStamp = Instant.EPOCH;
        Map<String, OperationHandler> handlers = ImmutableMap.of(OperationHandler.DELETE_OPERATION, handler,
                OperationHandler.ADD_OPERATION, handler);
        ArrayList<String> umc1Segments = new ArrayList<>(Arrays.asList("iphone", "macbook", "magic mouse"));
        Collections.reverse(umc1Segments);
        ArrayList<String> umc2Segments = new ArrayList<>(Arrays.asList("dakine bag", "dakine case", "dakine gloves"));
        Collections.reverse(umc2Segments);
        ArrayList<String> umc3Segments = new ArrayList<>(Arrays.asList("page closed", "page opened", "page reloaded"));
        Collections.reverse(umc3Segments);
        ArrayList<String> umc4Segments = new ArrayList<>(Arrays.asList("link highlighted", "link copied", "link clicked"));
        Collections.reverse(umc4Segments);
        UserModContainer<MapperUserModCommand> umc = new UserModContainer<>(new MapperUserModCommand(timeStamp,"11",OperationHandler.ADD_OPERATION,umc1Segments));
        UserModContainer<MapperUserModCommand> umc1 = new UserModContainer<>(new MapperUserModCommand(timeStamp,"11",OperationHandler.DELETE_OPERATION, umc2Segments));
        UserModContainer<MapperUserModCommand> umc3 = new UserModContainer<>(new MapperUserModCommand(timeStamp,"11",OperationHandler.DELETE_OPERATION, umc3Segments));
        UserModContainer<MapperUserModCommand> umc4 = new UserModContainer<>(new MapperUserModCommand(timeStamp,"11",OperationHandler.ADD_OPERATION, umc4Segments));
        Iterable<UserModContainer<MapperUserModCommand>> values = Arrays.asList(umc,umc1,umc3,umc4);
        Context context = mock(Context.class);
        
        when(context.getCounter("segmentreader", "reduce_counter")).thenReturn(mapRedCounter);
        
        AbstractCookieReducer reducer = createInstance(handlers);
        reducer.reduce(new Text("11"), values, context);

        verify(handler, times(2)).handle(umcCaptor.capture());
        verify(mapRedCounter, times(1)).increment(1);

        //aggregated items
        ArrayList<String> expectedFirstUmcSegments  = new ArrayList<>(Arrays.asList("magic mouse", "link copied", "macbook", "link clicked", "iphone", "link highlighted"));
        ArrayList<String> expectedSecondUmcSegments  = new ArrayList<>(Arrays.asList("page closed", "dakine bag", "page opened", "page reloaded", "dakine case", "dakine gloves"));
        MapperUserModCommand expectedUmc1 = new MapperUserModCommand(timeStamp,"11",OperationHandler.ADD_OPERATION,expectedFirstUmcSegments);
        MapperUserModCommand expectedUmc2 = new MapperUserModCommand(timeStamp,"11",OperationHandler.DELETE_OPERATION,expectedSecondUmcSegments);

        List<MapperUserModCommand> expectedUmcList = new LinkedList<>();
        expectedUmcList.add(expectedUmc1);
        expectedUmcList.add(expectedUmc2);
        assertThat(expectedUmcList, is(umcCaptor.getAllValues()));
    }
}