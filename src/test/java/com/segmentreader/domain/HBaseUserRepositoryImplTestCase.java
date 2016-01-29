package com.segmentreader.domain;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

public class HBaseUserRepositoryImplTestCase {

    @Test
    public void testUserRepositoryImplAddUserSuccess() throws IOException{
        HBaseUserRepositoryImpl userRepo = mock(HBaseUserRepositoryImpl.class);
        doNothing().when(userRepo).addUser("11", Arrays.asList("magic mouse"));
        
        userRepo.addUser("11", Arrays.asList("magic mouse"));
        
        verify(userRepo).addUser("11", Arrays.asList("magic mouse"));
    }
    
    
    @Test(expected=IOException.class)
    public void testUserRepositoryImplAddUserFailure() throws IOException{
        HBaseUserRepositoryImpl userRepo = mock(HBaseUserRepositoryImpl.class);
        doThrow(new IOException()).when(userRepo).addUser("11", Arrays.asList(""));
        
        userRepo.addUser("11", Arrays.asList(""));
        
        verify(userRepo).addUser("11", Arrays.asList(""));
    }
    
    @Test
    public void testUserRepositoryImplRemoveUser() throws IOException{
        HBaseUserRepositoryImpl userRepo = mock(HBaseUserRepositoryImpl.class);
        doNothing().when(userRepo).removeUser("22");
        
        userRepo.removeUser("22");
        
        verify(userRepo).removeUser("22");
    }

}
