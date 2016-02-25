package com.segmentreader.mapreduce;

import com.segmentreader.useroperations.OperationHandler;
import com.segmentreader.utils.TestHelper;
import org.junit.Test;
import static org.junit.Assert.*;

import java.time.Instant;
import java.util.Arrays;

public class MapperUserModCommandTestCase {

    private TestHelper<String> testHelper = new TestHelper<>();

    @Test
    public void testUMCComparingNotEqualTimestamp() {
        MapperUserModCommand umc11 = new MapperUserModCommand(testHelper.parseDateToInstant("2011-12-03T10:15:30+01:00"), "11", OperationHandler.ADD_OPERATION,
                testHelper.toArrayList(Arrays.asList("iphone","macbook", "magic mouse")));

        MapperUserModCommand umc22 = new MapperUserModCommand(testHelper.parseDateToInstant("2016-02-03T18:18:21+07:00"), "11", OperationHandler.ADD_OPERATION,
                testHelper.toArrayList(Arrays.asList("iphone","macbook", "magic mouse")));

        assertEquals(-1, umc11.compareTo(umc22)); //not equal
    }

    @Test
    public void testUMCComparingNotEqualUserId() {
        MapperUserModCommand umc11 = new MapperUserModCommand(Instant.EPOCH, "11", OperationHandler.ADD_OPERATION,
                testHelper.toArrayList(Arrays.asList("iphone","macbook", "magic mouse")));

        MapperUserModCommand umc22 = new MapperUserModCommand(Instant.EPOCH, "22", OperationHandler.ADD_OPERATION,
                testHelper.toArrayList(Arrays.asList("iphone","macbook", "magic mouse")));

        assertEquals(-1, umc11.compareTo(umc22)); //not equal
    }

    @Test
    public void testUMCComparingNotEqualOperation() {
        MapperUserModCommand umc11 = new MapperUserModCommand(Instant.EPOCH, "11", OperationHandler.ADD_OPERATION,
                testHelper.toArrayList(Arrays.asList("iphone","macbook", "magic mouse")));

        MapperUserModCommand umc22 = new MapperUserModCommand(Instant.EPOCH, "11", OperationHandler.DELETE_OPERATION,
                testHelper.toArrayList(Arrays.asList("iphone","macbook", "magic mouse")));

        assertEquals(-1, umc11.compareTo(umc22)); //not equal
    }

    @Test
    public void testUMCComparingNotEqualSegmentsList() {
        MapperUserModCommand umc11 = new MapperUserModCommand(Instant.EPOCH, "11", OperationHandler.ADD_OPERATION,
                testHelper.toArrayList(Arrays.asList("iphone","macbook", "magic mouse")));

        MapperUserModCommand umc22 = new MapperUserModCommand(Instant.EPOCH, "11", OperationHandler.ADD_OPERATION,
                testHelper.toArrayList(Arrays.asList("android", "android", "android", "chromebook", "normal mouse")));

        assertEquals(1, umc11.compareTo(umc22)); //not equal
    }


    @Test
    public void testUMCComparingEqual() {
        MapperUserModCommand umc11 = new MapperUserModCommand(Instant.EPOCH, "11", OperationHandler.ADD_OPERATION,
                testHelper.toArrayList(Arrays.asList("iphone","macbook", "magic mouse")));

        MapperUserModCommand umc22 = new MapperUserModCommand(Instant.EPOCH, "11", OperationHandler.ADD_OPERATION,
                testHelper.toArrayList(Arrays.asList("iphone","macbook", "magic mouse")));

        assertEquals(0, umc11.compareTo(umc22)); // equal
    }
}
