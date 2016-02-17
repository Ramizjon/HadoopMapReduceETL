package com.segmentreader.mapreduce;

import com.segmentreader.useroperations.OperationHandler;
import org.junit.Test;
import static org.junit.Assert.*;

import java.time.Instant;
import java.util.ArrayList;

public class MapperUserModCommandTestCase {

    @Test
    public void testUMCComparingNotEqual() {
        MapperUserModCommand umc11 = new MapperUserModCommand(Instant.EPOCH, "11", OperationHandler.ADD_OPERATION, new ArrayList<String>() {{
            add("iphone");
            add("macbook");
            add("magic mouse");
        }});
        MapperUserModCommand umc22 = new MapperUserModCommand(Instant.EPOCH, "22", OperationHandler.DELETE_OPERATION, new ArrayList<String>() {{
            add("android");
            add("android");
            add("android");
            add("chromebook");
            add("normal mouse");
        }});

        assertEquals(1, umc11.compareTo(umc22)); //not equal
    }

    @Test
    public void testUMCComparingEqual() {
        MapperUserModCommand umc11 = new MapperUserModCommand(Instant.EPOCH, "11", OperationHandler.ADD_OPERATION, new ArrayList<String>() {{
            add("iphone");
            add("macbook");
            add("magic mouse");
        }});
        MapperUserModCommand umc22 = new MapperUserModCommand(Instant.EPOCH, "11", OperationHandler.ADD_OPERATION, new ArrayList<String>() {{
            add("iphone");
            add("macbook");
            add("magic mouse");
        }});

        assertEquals(0, umc11.compareTo(umc22)); // equal
    }
}
