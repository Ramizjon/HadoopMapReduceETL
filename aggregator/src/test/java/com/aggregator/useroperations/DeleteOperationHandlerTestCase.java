package com.aggregator.useroperations;

import com.aggregator.domain.UserRepository;
import com.common.mapreduce.ReducerUserModCommand;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static org.mockito.Mockito.*;


public class DeleteOperationHandlerTestCase {

    private static final String timestampValue = Instant.EPOCH.toString();

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
        Map<String, String> map11 = ImmutableMap.of("iphone", timestampValue, "macbook", timestampValue,
                "magic mouse", timestampValue);
        ReducerUserModCommand userMod = new ReducerUserModCommand("11", OperationHandler.ADD_OPERATION, map11);
        deleteHandler.handle(userMod);

        verify(userRepo, times(1)).removeUser(userMod);
    }

}
