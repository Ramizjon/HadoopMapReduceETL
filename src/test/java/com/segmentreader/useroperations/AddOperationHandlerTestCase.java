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
    public void testAddHandlerWithValidSegments() throws IOException, ParseException {
        // arrange
        UserRepository repo = mock(UserRepository.class);
        AddOperationHandler handler = createInstance(repo);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS+hh:mm");
        Date parsedTimeStamp = dateFormat.parse("2014-08-22 15:02:51:580+12:15");
        Timestamp timestamp = new Timestamp(parsedTimeStamp.getTime());
        UserModCommand nonEmptyUserMod = new UserModCommand(timestamp, "user33", "add", Arrays.asList("website click"));
        // act
        handler.handle(nonEmptyUserMod);
        
        //assert
        verify(repo, times(1)).addUser(timestamp,"user33", Arrays.asList("website click"));
    }
   
   
   
}
