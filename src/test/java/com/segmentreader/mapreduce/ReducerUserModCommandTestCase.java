package com.segmentreader.mapreduce;

import com.google.common.collect.ImmutableMap;
import com.segmentreader.useroperations.OperationHandler;
import org.apache.commons.collections.ListUtils;
import org.junit.Test;
import java.util.AbstractMap.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ReducerUserModCommandTestCase {

    private static final String timestampValue = Instant.EPOCH.toString();

    @Test
    public void testUMCComparingNotEqualUserId() {
        Map<String, String> map11 = ImmutableMap.of("iphone", timestampValue, "macbook", timestampValue,
                "magic mouse", timestampValue);

        ReducerUserModCommand umc11 = new ReducerUserModCommand("11", OperationHandler.ADD_OPERATION, map11);
        ReducerUserModCommand umc22 = new ReducerUserModCommand("22", OperationHandler.ADD_OPERATION, map11);

        assertEquals(1, umc11.compareTo(umc22)); //not equal
    }

    @Test
    public void testUMCComparingNotEqualOperation() {
        Map<String, String> map11 = ImmutableMap.of("iphone", timestampValue, "macbook", timestampValue,
                "magic mouse", timestampValue);

        ReducerUserModCommand umc11 = new ReducerUserModCommand("11", OperationHandler.ADD_OPERATION, map11);
        ReducerUserModCommand umc22 = new ReducerUserModCommand("22", OperationHandler.DELETE_OPERATION, map11);

        assertEquals(1, umc11.compareTo(umc22)); //not equal
    }

    @Test
    public void testUMCComparingNotEqualSegmentTimestamps() {
        Map<String, String> map11 = ImmutableMap.of("iphone", timestampValue, "macbook", timestampValue,
                "magic mouse", timestampValue);
        ReducerUserModCommand umc11 = new ReducerUserModCommand("11", OperationHandler.ADD_OPERATION, map11);

        Map<String, String> map22 = ImmutableMap.of("android", timestampValue, "chromebook", timestampValue,
                "normal mouse", timestampValue);
        ReducerUserModCommand umc22 = new ReducerUserModCommand("11", OperationHandler.ADD_OPERATION, map22);

        assertEquals(1, umc11.compareTo(umc22)); //not equal
    }

    @Test
    public void testUMCComparingEqual() {
        Map<String, String> map11 = ImmutableMap.of("iphone", timestampValue, "macbook", timestampValue,
                "magic mouse", timestampValue);

        ReducerUserModCommand umc11 = new ReducerUserModCommand("22", OperationHandler.ADD_OPERATION, map11);
        ReducerUserModCommand umc22 = new ReducerUserModCommand("22", OperationHandler.ADD_OPERATION, map11);

        assertEquals(0, umc11.compareTo(umc22)); // equal
    }

}
