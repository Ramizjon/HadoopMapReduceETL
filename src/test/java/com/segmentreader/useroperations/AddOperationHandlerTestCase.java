package com.segmentreader.useroperations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import com.segmentreader.domain.UserRepository;
import com.segmentreader.mapreduce.UserModCommand;
import com.segmentreader.useroperations.AddOperationHandler;

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
        UserModCommand nonEmptyUserMod = new UserModCommand("user33", "add", Arrays.asList("website click"));
        // act
        handler.handle(nonEmptyUserMod);
        
        //assert
        verify(repo, times(1)).addUser("user33", Arrays.asList("website click"));
    }
   
   
   
}
