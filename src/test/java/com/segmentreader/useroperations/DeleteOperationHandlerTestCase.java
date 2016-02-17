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
        Instant timestamp = Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse("2011-12-03T10:15:30+01:00"));
        ReducerUserModCommand userMod = new ReducerUserModCommand("user22", "add",
                new AbstractMap.SimpleEntry<ArrayList<String>, Instant>(new ArrayList<>(Arrays.asList("website click")), timestamp));
        
        deleteHandler.handle(userMod);
        
        verify(userRepo, times(1)).removeUser("user22");
    }

}
