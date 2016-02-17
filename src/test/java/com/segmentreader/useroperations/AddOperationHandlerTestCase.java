package com.segmentreader.useroperations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;

import com.segmentreader.mapreduce.MapperUserModCommand;
import com.segmentreader.mapreduce.ReducerUserModCommand;
import org.junit.Test;

import com.segmentreader.domain.UserRepository;

public class AddOperationHandlerTestCase {

    private AddOperationHandler createInstance(UserRepository repo) {
        return new AddOperationHandler() {
            @Override
            protected UserRepository getRepoInstance() {
              return repo;      
            }   
        };
    }
    
    @Test
    public void testAddHandlerWithValidSegments() throws IOException {
        // arrange
        UserRepository repo = mock(UserRepository.class);
        AddOperationHandler handler = createInstance(repo);
        Instant timestamp = Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse("2011-12-03T10:15:30+01:00"));
        ReducerUserModCommand nonEmptyUserMod = new ReducerUserModCommand("user33", "add",
                new AbstractMap.SimpleEntry<ArrayList<String>, Instant>(new ArrayList<>(Arrays.asList("website click")), timestamp));
        // act
        handler.handle(nonEmptyUserMod);
        
        //assert
        verify(repo, times(1)).addUser(nonEmptyUserMod);
    }
   
   
   
}
