package com.segmentreader.useroperations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import com.segmentreader.domain.UserRepository;
import com.segmentreader.mapreduce.UserModCommand;


public class DeleteOperationHandlerTestCase {

    private DeleteOperationHandler createInstance(UserRepository repo) {
        return new DeleteOperationHandler() {
            @Override
            protected UserRepository getRepoInstance() {
                return repo;
            }
        };
    }

    @Test
    public void testDeleteHandlerWithValidSegments() throws IOException {
        UserRepository userRepo = mock(UserRepository.class);
        DeleteOperationHandler deleteHandler = createInstance(userRepo);
        UserModCommand userMod = new UserModCommand("user22", "add", Arrays.asList("website click"));
        
        deleteHandler.handle(userMod);
        
        verify(userRepo, times(1)).removeUser("user22");
    }

}
