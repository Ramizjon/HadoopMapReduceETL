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

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.segmentreader.useroperations.OperationHandler;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

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
        ArgumentCaptor<UserModCommand> umcCaptor = ArgumentCaptor.forClass(UserModCommand.class);
        Counter mapRedCounter = mock(Counter.class);
        Instant timeStamp = Instant.EPOCH;
        Map<String, OperationHandler> handlers = ImmutableMap.of(OperationHandler.DELETE_OPERATION, handler,
                OperationHandler.ADD_OPERATION, handler);
        List<String> umc1Segments = Arrays.asList("iphone", "macbook", "magic mouse");
        Collections.reverse(umc1Segments);
        List<String> umc2Segments = Arrays.asList("dakine bag", "dakine case", "dakine gloves");
        Collections.reverse(umc2Segments);
        List<String> umc3Segments = Arrays.asList("page closed", "page opened", "page reloaded");
        Collections.reverse(umc3Segments);
        List<String> umc4Segments = Arrays.asList("link highlighted", "link copied", "link clicked");
        Collections.reverse(umc4Segments);
        UserModCommand umc = new UserModCommand(timeStamp,"11",OperationHandler.ADD_OPERATION,umc1Segments);
        UserModCommand umc1 = new UserModCommand(timeStamp,"11",OperationHandler.DELETE_OPERATION, umc2Segments);
        UserModCommand umc3 = new UserModCommand(timeStamp,"11",OperationHandler.DELETE_OPERATION, umc3Segments);
        UserModCommand umc4 = new UserModCommand(timeStamp,"11",OperationHandler.ADD_OPERATION, umc4Segments);
        Iterable<UserModCommand> values = Arrays.asList(umc,umc1,umc3,umc4);
        Context context = mock(Context.class);
        
        when(context.getCounter("segmentreader", "reduce_counter")).thenReturn(mapRedCounter);
        
        AbstractCookieReducer reducer = createInstance(handlers);
        reducer.reduce(new Text("11"), values, context);

        verify(handler, times(2)).handle(umcCaptor.capture());
        verify(mapRedCounter, times(1)).increment(1);

        //aggregated items
        List<String> expectedFirstUmcSegments  = Arrays.asList("magic mouse", "link copied", "macbook", "link clicked", "iphone", "link highlighted");
        List<String> expectedSecondUmcSegments  = Arrays.asList("page closed", "dakine bag", "page opened", "page reloaded", "dakine case", "dakine gloves");
        UserModCommand expectedUmc1 = new UserModCommand(timeStamp,"11",OperationHandler.ADD_OPERATION,expectedFirstUmcSegments);
        UserModCommand expectedUmc2 = new UserModCommand(timeStamp,"11",OperationHandler.DELETE_OPERATION,expectedSecondUmcSegments);

        List<UserModCommand> expectedUmcList = new LinkedList<>();
        expectedUmcList.add(expectedUmc1);
        expectedUmcList.add(expectedUmc2);
        assertThat(expectedUmcList, is(umcCaptor.getAllValues()));
    }
}