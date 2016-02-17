package com.segmentreader.mapreduce;

import com.segmentreader.useroperations.OperationHandler;
import org.apache.commons.collections.ListUtils;
import org.junit.Test;
import java.util.AbstractMap.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;

public class ReducerUserModCommandTestCase {

    @Test
    public void testUMCComparingNotEqual() {
        ReducerUserModCommand umc11 = new ReducerUserModCommand("22", OperationHandler.ADD_OPERATION,
                new SimpleEntry<ArrayList<String>, Instant>(new ArrayList<String>(
                        Arrays.asList("iphone","macbook", "magic mouse")),Instant.EPOCH));

        ReducerUserModCommand umc22 = new ReducerUserModCommand("22", OperationHandler.DELETE_OPERATION,
                new SimpleEntry<ArrayList<String>, Instant>(new ArrayList<String>(
                Arrays.asList("android","chromebook", "normal mouse")),Instant.EPOCH));

        assertEquals(1, umc11.compareTo(umc22)); //not equal
    }

    @Test
    public void testUMCComparingEqual() {
        ReducerUserModCommand umc11 = new ReducerUserModCommand("22", OperationHandler.DELETE_OPERATION,
                new SimpleEntry<ArrayList<String>, Instant>(new ArrayList<String>(
                        Arrays.asList("android","chromebook", "normal mouse")),Instant.EPOCH));

        ReducerUserModCommand umc22 = new ReducerUserModCommand("22", OperationHandler.DELETE_OPERATION,
                new SimpleEntry<ArrayList<String>, Instant>(new ArrayList<String>(
                        Arrays.asList("android","chromebook", "normal mouse")),Instant.EPOCH));

        assertEquals(0, umc11.compareTo(umc22)); // equal
    }

}
