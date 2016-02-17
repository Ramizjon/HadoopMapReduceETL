package com.segmentreader.mapreduce;


import com.segmentreader.useroperations.OperationHandler;
import com.segmentreader.utils.UserModContainer;
import org.apache.commons.collections.ListUtils;
import org.junit.Test;
import java.util.AbstractMap.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;

public class UserModContainerTestCase {

    @Test
    public void testUMCComparingNotEqual() {
        ReducerUserModCommand umc11 = new ReducerUserModCommand("21", OperationHandler.ADD_OPERATION,
                new SimpleEntry<ArrayList<String>, Instant>(new ArrayList<String>(
                        Arrays.asList("iphone","macbook", "magic mouse")),Instant.EPOCH));
        UserModContainer<ReducerUserModCommand> umc1 = new UserModContainer<>(umc11);

        ReducerUserModCommand umc22 = new ReducerUserModCommand("22", OperationHandler.DELETE_OPERATION,
                new SimpleEntry<ArrayList<String>, Instant>(new ArrayList<String>(
                        Arrays.asList("android","chromebook", "normal mouse")),Instant.EPOCH));
        UserModContainer<ReducerUserModCommand> umc2 = new UserModContainer<>(umc22);

        assertEquals(1, umc1.compareTo(umc2)); //not equal
    }

    @Test
    public void testUMCComparingEqual() {
        ReducerUserModCommand umc11 = new ReducerUserModCommand("22", OperationHandler.ADD_OPERATION,
                new SimpleEntry<ArrayList<String>, Instant>(new ArrayList<String>(
                        Arrays.asList("iphone","macbook", "magic mouse")),Instant.EPOCH));
        UserModContainer<ReducerUserModCommand> umc1 = new UserModContainer<>(umc11);

        ReducerUserModCommand umc22 = new ReducerUserModCommand("22", OperationHandler.ADD_OPERATION,
                new SimpleEntry<ArrayList<String>, Instant>(new ArrayList<String>(
                        Arrays.asList("iphone","macbook", "magic mouse")),Instant.EPOCH));
        UserModContainer<ReducerUserModCommand> umc2 = new UserModContainer<>(umc22);

        assertEquals(0, umc1.compareTo(umc2)); // equal
    }
}
