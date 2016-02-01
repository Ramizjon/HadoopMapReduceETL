package com.segmentreader.dataformats;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.segmentreader.mapreduce.UserModCommand;

public class ConvertorImplTestCase {

    @Test
    public void testConvertorWithValidInput() throws IOException {
       String input = "14,add,generatedlink,closedtab";
       Convertor convertor = new ConvertorImpl();
       List<String> expectedSegments = Arrays.asList(input.split(",")).subList(2, 4);
       
       UserModCommand expected = new UserModCommand("14","add",expectedSegments);
       UserModCommand umc = convertor.convert(input);
       
       assertEquals(expected, umc);
    }
    
    @Test(expected=IOException.class)
    public void testConvertorWithInvalidInput() throws IOException {
       String input = "14,add";
       Convertor convertor = new ConvertorImpl();
       
       convertor.convert(input);
    }

}
