package com.segmentreader.domain;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.PUT;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Test;

public class HBaseUserRepositoryImplTestCase {

    private HBaseUserRepositoryImpl createRepository(HTable hTable)
            throws IOException {
        return new HBaseUserRepositoryImpl() {
            @Override
            protected HTable createHTable() throws IOException {
                return hTable;
            }
        };
    }

    @Test
    public void testUserRepositoryImplAddUserSingleRecord() throws IOException {
        HTable hTable = mock(HTable.class);
        HBaseUserRepositoryImpl userRepo = createRepository(hTable);
        userRepo.setBufferSize(1);
        List<String> list = Arrays.asList("magic mouse");
        userRepo.addUser("11", list);
        
        verify(hTable, times(1)).put(any(Put.class));
    }

    @Test
    public void testUserRepositoryImplAddUserMultipleRecords()
            throws IOException {
        HTable hTable = mock(HTable.class);
        HBaseUserRepositoryImpl userRepo = createRepository(hTable);
        userRepo.setBufferSize(2);

        List<String> list = Arrays.asList("magic mouse");
        userRepo.addUser("11", list);
        verify(hTable, times(0)).put(any(Put.class));
        userRepo.addUser("23", list);
        verify(hTable, times(2)).put(any(Put.class));
    }

    @Test
    public void testUserRepositoryImplDeleteUserSingleRecord() throws IOException {
        HTable hTable = mock(HTable.class);
        HBaseUserRepositoryImpl userRepo = createRepository(hTable);
        userRepo.setBufferSize(1);;
        userRepo.removeUser("user1");
        verify(hTable, times(1)).delete(any(Delete.class));
    }


}
