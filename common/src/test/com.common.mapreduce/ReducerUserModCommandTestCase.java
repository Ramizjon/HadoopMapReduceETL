package com.common.mapreduce;

import com.common.mapreduce.ReducerUserModCommand;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ReducerUserModCommandTestCase {

    private static final String TIMESTAMP_VALUE = Instant.EPOCH.toString();
    private static final String ADD_OPERATION = "add";
    private static final String DELETE_OPERATION = "delete";

    @Test
    public void testUMCComparingNotEqualUserId() {
        Map<String, String> map11 = ImmutableMap.of("iphone", TIMESTAMP_VALUE, "macbook", TIMESTAMP_VALUE,
                "magic mouse", TIMESTAMP_VALUE);

        ReducerUserModCommand umc11 = new ReducerUserModCommand("11", ADD_OPERATION, map11);
        ReducerUserModCommand umc22 = new ReducerUserModCommand("22", ADD_OPERATION, map11);

        assertEquals(1, umc11.compareTo(umc22)); //not equal
    }

    @Test
    public void testUMCComparingNotEqualOperation() {
        Map<String, String> map11 = ImmutableMap.of("iphone", TIMESTAMP_VALUE, "macbook", TIMESTAMP_VALUE,
                "magic mouse", TIMESTAMP_VALUE);

        ReducerUserModCommand umc11 = new ReducerUserModCommand("11", ADD_OPERATION, map11);
        ReducerUserModCommand umc22 = new ReducerUserModCommand("22", DELETE_OPERATION, map11);

        assertEquals(1, umc11.compareTo(umc22)); //not equal
    }

    @Test
    public void testUMCComparingNotEqualSegmentTimestamps() {
        Map<String, String> map11 = ImmutableMap.of("iphone", TIMESTAMP_VALUE, "macbook", TIMESTAMP_VALUE,
                "magic mouse", TIMESTAMP_VALUE);
        ReducerUserModCommand umc11 = new ReducerUserModCommand("11", ADD_OPERATION, map11);

        Map<String, String> map22 = ImmutableMap.of("android", TIMESTAMP_VALUE, "chromebook", TIMESTAMP_VALUE,
                "normal mouse", TIMESTAMP_VALUE);
        ReducerUserModCommand umc22 = new ReducerUserModCommand("11", ADD_OPERATION, map22);

        assertEquals(1, umc11.compareTo(umc22)); //not equal
    }

    @Test
    public void testUMCComparingEqual() {
        Map<String, String> map11 = ImmutableMap.of("iphone", TIMESTAMP_VALUE, "macbook", TIMESTAMP_VALUE,
                "magic mouse", TIMESTAMP_VALUE);

        ReducerUserModCommand umc11 = new ReducerUserModCommand("22", ADD_OPERATION, map11);
        ReducerUserModCommand umc22 = new ReducerUserModCommand("22", ADD_OPERATION, map11);

        assertEquals(0, umc11.compareTo(umc22)); // equal
    }

}
