package com.segmentreader.useroperations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
        LocalDateTime localDateTime = LocalDateTime.parse(Instant.EPOCH.toString(), formatter);
        Instant timestamp = localDateTime.toInstant(ZoneOffset.UTC);
        UserModCommand nonEmptyUserMod = new UserModCommand(timestamp, "user33", "add", Arrays.asList("website click"));
        // act
        handler.handle(nonEmptyUserMod);
        
        //assert
        verify(repo, times(1)).addUser(timestamp,"user33", Arrays.asList("website click"));
    }
   
   
   
}
