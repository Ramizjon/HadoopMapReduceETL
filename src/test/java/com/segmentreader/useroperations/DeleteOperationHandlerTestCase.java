package com.segmentreader.useroperations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

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
    public void testDeleteHandlerWithValidSegments() throws IOException, ParseException {
        UserRepository userRepo = mock(UserRepository.class);
        DeleteOperationHandler deleteHandler = createInstance(userRepo);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS+hh:mm");
        Date parsedTimeStamp = dateFormat.parse("2014-08-22 15:02:51:580+12:15");
        Timestamp timestamp = new Timestamp(parsedTimeStamp.getTime());
        UserModCommand userMod = new UserModCommand(timestamp, "user22", "add", Arrays.asList("website click"));
        
        deleteHandler.handle(userMod);
        
        verify(userRepo, times(1)).removeUser("user22");
    }

}
