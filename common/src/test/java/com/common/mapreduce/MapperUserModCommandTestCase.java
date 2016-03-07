package com.common.mapreduce;

import org.junit.Test;
import static org.junit.Assert.*;

import java.time.Instant;
import java.util.Arrays;

public class MapperUserModCommandTestCase {

    private TestHelper<String> testHelper = new TestHelper<>();
    private final String ADD_OPERATION = "add";
    private final String DELETE_OPERATION = "delete";
    private final String EPOCH_TIME = Instant.EPOCH.toString();

    @Test
    public void testUMCComparingNotEqualTimestamp() {
        MapperUserModCommand umc11 = new MapperUserModCommand("2011-12-03T10:15:30+01:00", "11", ADD_OPERATION,
                testHelper.toArrayList(Arrays.asList("iphone","macbook", "magic mouse")));

        MapperUserModCommand umc22 = new MapperUserModCommand("2016-02-03T18:18:21+07:00", "11", ADD_OPERATION,
                testHelper.toArrayList(Arrays.asList("iphone","macbook", "magic mouse")));

        assertEquals(-1, umc11.compareTo(umc22)); //not equal
    }

    @Test
    public void testUMCComparingNotEqualUserId() {
        MapperUserModCommand umc11 = new MapperUserModCommand(EPOCH_TIME, "11", ADD_OPERATION,
                testHelper.toArrayList(Arrays.asList("iphone","macbook", "magic mouse")));

        MapperUserModCommand umc22 = new MapperUserModCommand(EPOCH_TIME, "22", ADD_OPERATION,
                testHelper.toArrayList(Arrays.asList("iphone","macbook", "magic mouse")));

        assertEquals(-1, umc11.compareTo(umc22)); //not equal
    }

    @Test
    public void testUMCComparingNotEqualOperation() {
        MapperUserModCommand umc11 = new MapperUserModCommand(EPOCH_TIME, "11", ADD_OPERATION,
                testHelper.toArrayList(Arrays.asList("iphone","macbook", "magic mouse")));

        MapperUserModCommand umc22 = new MapperUserModCommand(EPOCH_TIME, "11", DELETE_OPERATION,
                testHelper.toArrayList(Arrays.asList("iphone","macbook", "magic mouse")));

        assertEquals(-1, umc11.compareTo(umc22)); //not equal
    }

    @Test
    public void testUMCComparingNotEqualSegmentsList() {
        MapperUserModCommand umc11 = new MapperUserModCommand(EPOCH_TIME, "11", ADD_OPERATION,
                testHelper.toArrayList(Arrays.asList("iphone","macbook", "magic mouse")));

        MapperUserModCommand umc22 = new MapperUserModCommand(EPOCH_TIME, "11", ADD_OPERATION,
                testHelper.toArrayList(Arrays.asList("android", "android", "android", "chromebook", "normal mouse")));

        assertEquals(1, umc11.compareTo(umc22)); //not equal
    }


    @Test
    public void testUMCComparingEqual() {
        MapperUserModCommand umc11 = new MapperUserModCommand(EPOCH_TIME, "11", ADD_OPERATION,
                testHelper.toArrayList(Arrays.asList("iphone","macbook", "magic mouse")));

        MapperUserModCommand umc22 = new MapperUserModCommand(EPOCH_TIME, "11", ADD_OPERATION,
                testHelper.toArrayList(Arrays.asList("iphone","macbook", "magic mouse")));

        assertEquals(0, umc11.compareTo(umc22)); // equal
    }
}