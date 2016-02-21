package com.segmentreader.mapreduce;


import com.google.common.collect.ImmutableMap;
import com.segmentreader.useroperations.OperationHandler;
import com.segmentreader.utils.UserModContainer;
import org.apache.commons.collections.ListUtils;
import org.junit.Test;
import java.util.AbstractMap.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.*;

public class UserModContainerTestCase {

    private static final String timestampValue = Instant.EPOCH.toString();

    @Test
    public void testUMCComparingNotEqual() {
        Map<String, String> map11 = ImmutableMap.of("iphone", timestampValue, "macbook", timestampValue,
                "magic mouse", timestampValue);
        ReducerUserModCommand umc11 = new ReducerUserModCommand("11", OperationHandler.ADD_OPERATION, map11);
        UserModContainer<ReducerUserModCommand> umc1 = new UserModContainer<>(umc11);

        Map<String, String> map22 = ImmutableMap.of("android", timestampValue, "chromebook", timestampValue,
                "normal mouse", timestampValue);
        ReducerUserModCommand umc22 = new ReducerUserModCommand("22", OperationHandler.ADD_OPERATION, map11);
        UserModContainer<ReducerUserModCommand> umc2 = new UserModContainer<>(umc22);

        assertEquals(1, umc1.compareTo(umc2)); //not equal
    }

    @Test
    public void testUMCComparingEqual() {
        Map<String, String> map11 = ImmutableMap.of("iphone", timestampValue, "macbook", timestampValue,
                "magic mouse", timestampValue);
        ReducerUserModCommand umc11 = new ReducerUserModCommand("11", OperationHandler.ADD_OPERATION, map11);
        UserModContainer<ReducerUserModCommand> umc1 = new UserModContainer<>(umc11);

        Map<String, String> map22 = ImmutableMap.of("iphone", timestampValue, "macbook", timestampValue,
                "maigc mouse", timestampValue);
        ReducerUserModCommand umc22 = new ReducerUserModCommand("11", OperationHandler.ADD_OPERATION, map11);
        UserModContainer<ReducerUserModCommand> umc2 = new UserModContainer<>(umc22);

        assertEquals(0, umc1.compareTo(umc2)); // equal
    }
}
