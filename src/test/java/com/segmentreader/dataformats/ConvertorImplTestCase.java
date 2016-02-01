package com.segmentreader.dataformats;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import com.segmentreader.mapreduce.UserModCommand;

public class ConvertorImplTestCase {

    @Test
    public void testConvertorWithValidInput() throws IOException {
       String input = "14,add,generatedlink,closedtab";
       Convertor convertor = new ConvertorImpl();
       List<String> expectedSegments = new LinkedList(){{
           add("generatedlink");
           add("closedtab");
       }};
       
       UserModCommand umc = convertor.convert(input);
       
       assertEquals("14", umc.getUserId());
       assertEquals("add", umc.getCommand());
       assertEquals(expectedSegments,umc.getSegments());
    }
    
    @Test(expected=IOException.class)
    public void testConvertorWithInValidInput() throws IOException {
       String input = "14,add";
       Convertor convertor = new ConvertorImpl();
       convertor.convert(input);
    }

}
