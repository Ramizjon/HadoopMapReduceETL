package com.segmentreader.mapreduce;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.segmentreader.useroperations.OperationHandler;

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
        OperationHandler handler = mock(OperationHandler.class);
        Counter mapRedCounter = mock(Counter.class);
        Map<String, OperationHandler> handlers = ImmutableMap.of(OperationHandler.DELETE_OPERATION, handler,
                OperationHandler.ADD_OPERATION, handler);
        UserModCommand umc = new UserModCommand("11",OperationHandler.ADD_OPERATION, 
                Arrays.asList("iphone", "macbook", "magic mouse"));
        UserModCommand umc1 = new UserModCommand("11",OperationHandler.DELETE_OPERATION, 
                Arrays.asList("dakine bag", "dakine case", "dakine gloves"));

        Iterable<UserModCommand> values = Arrays.asList(umc,umc1);
        Context context = mock(Context.class);
        
        when(context.getCounter("segmentreader", "reduce_counter")).thenReturn(mapRedCounter);
        
        AbstractCookieReducer reducer = createInstance(handlers);
        reducer.reduce(new Text("1"), values, context);
 
        verify(handler, times(2)).handle(any(UserModCommand.class));
        verify(mapRedCounter, times(1)).increment(1);
    }
}
