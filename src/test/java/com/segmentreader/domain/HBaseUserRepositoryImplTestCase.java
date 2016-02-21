package com.segmentreader.domain;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.segmentreader.mapreduce.MapperUserModCommand;
import com.segmentreader.mapreduce.ReducerUserModCommand;
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
//        HTable hTable = mock(HTable.class);
//        HBaseUserRepositoryImpl userRepo = createRepository(hTable);
//        userRepo.setBufferSize(1);
//        Instant timestamp = parseDateToInstant("2011-12-03T10:15:30+01:00");
//        List<String> list = Arrays.asList("magic mouse");
//
//        userRepo.addUser(new ReducerUserModCommand("11","add",
//                new AbstractMap.SimpleEntry<ArrayList<String>, Instant>(new ArrayList<>(list),timestamp)));
//
//        verify(hTable, times(1)).put(any(Put.class));
    }

    @Test
    public void testUserRepositoryImplAddUserMultipleRecords()
            throws IOException {
//        HTable hTable = mock(HTable.class);
//        Instant timestamp = parseDateToInstant("2011-12-03T10:15:30+01:00");
//        HBaseUserRepositoryImpl userRepo = createRepository(hTable);
//        userRepo.setBufferSize(2);
//
//        ArrayList<String> list = new ArrayList<>(Arrays.asList("magic mouse"));
//        userRepo.addUser(new ReducerUserModCommand("11","add",
//                new AbstractMap.SimpleEntry<ArrayList<String>, Instant>(list,timestamp)));
//        verify(hTable, times(0)).put(any(Put.class));
//        userRepo.addUser(new ReducerUserModCommand("23","add",
//                new AbstractMap.SimpleEntry<ArrayList<String>, Instant>(list,timestamp)));
//        verify(hTable, times(2)).put(any(Put.class));
    }

    @Test
    public void testUserRepositoryImplDeleteUserSingleRecord() throws IOException {
        HTable hTable = mock(HTable.class);
        HBaseUserRepositoryImpl userRepo = createRepository(hTable);
        userRepo.setBufferSize(1);;
        userRepo.removeUser("user1");
        verify(hTable, times(1)).delete(any(Delete.class));
    }

    private Instant parseDateToInstant (String date) {
        return Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(date));
     }
}
