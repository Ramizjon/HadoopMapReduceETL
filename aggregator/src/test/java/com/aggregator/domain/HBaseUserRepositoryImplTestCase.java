package com.aggregator.domain;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.*;

import com.common.mapreduce.ReducerUserModCommand;
import com.google.common.collect.ImmutableMap;
import com.aggregator.useroperations.OperationHandler;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Test;

public class HBaseUserRepositoryImplTestCase {

    private static final String timestampValue = "2011-12-03T10:15:30+01:00";

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
        String timestamp = "2011-12-03T10:15:30+01:00";
        Map<String, String> map11 = ImmutableMap.of("iphone", timestamp.toString(), "macbook", timestamp.toString(),
                "magic mouse", timestamp.toString());
        ReducerUserModCommand umc11 = new ReducerUserModCommand("11", OperationHandler.ADD_OPERATION, map11);

        userRepo.addUser(umc11);

        verify(hTable, times(1)).put(any(Put.class));
    }

    @Test
    public void testUserRepositoryImplAddUserMultipleRecords()
            throws IOException {
        HTable hTable = mock(HTable.class);

        HBaseUserRepositoryImpl userRepo = createRepository(hTable);
        userRepo.setBufferSize(2);
        Map<String, String> map11 = ImmutableMap.of("iphone", timestampValue, "macbook", timestampValue,
                "magic mouse", timestampValue);
        ReducerUserModCommand umc11 = new ReducerUserModCommand("11", OperationHandler.ADD_OPERATION, map11);
        Map<String, String> map22 = ImmutableMap.of("android", timestampValue, "chromebook", timestampValue,
                "normal mouse", timestampValue);
        ReducerUserModCommand umc22 = new ReducerUserModCommand("11", OperationHandler.ADD_OPERATION, map22);

        userRepo.addUser(umc11);
        userRepo.addUser(umc22);

        verify(hTable, times(2)).put(any(Put.class));
    }

    @Test
    public void testUserRepositoryImplDeleteUserSingleRecord() throws IOException {
        HTable hTable = mock(HTable.class);
        Map<String, String> map11 = ImmutableMap.of("iphone", timestampValue, "macbook", timestampValue,
                "magic mouse", timestampValue);
        ReducerUserModCommand umc11 = new ReducerUserModCommand("11", OperationHandler.ADD_OPERATION, map11);
        HBaseUserRepositoryImpl userRepo = createRepository(hTable);

        userRepo.setBufferSize(1);
        userRepo.removeUser(umc11);

        verify(hTable, times(3)).delete(any(Delete.class));
    }

}
