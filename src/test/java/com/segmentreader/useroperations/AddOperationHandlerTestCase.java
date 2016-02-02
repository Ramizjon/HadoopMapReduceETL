package com.segmentreader.useroperations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

import org.junit.Test;

import com.segmentreader.domain.UserRepository;
import com.segmentreader.mapreduce.UserModCommand;

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
        UserModCommand nonEmptyUserMod = new UserModCommand(timestamp, "user33", "add", Arrays.asList("website click"));
        // act
        handler.handle(nonEmptyUserMod);
        
        //assert
        verify(repo, times(1)).addUser(timestamp,"user33", Arrays.asList("website click"));
    }
   
   
   
}
