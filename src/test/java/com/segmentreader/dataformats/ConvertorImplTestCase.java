package com.segmentreader.dataformats;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.segmentreader.mapreduce.UserModCommand;

public class ConvertorImplTestCase {

    private static final String inputDate = "2011-12-03T10:15:30+01:00";
    
    @Test
    public void testConvertorWithValidInput() throws IOException {
       String input = inputDate+",14,add,generatedlink,closedtab";
       Convertor convertor = new ConvertorImpl();
       List<String> expectedSegments = Arrays.asList(input.split(",")).subList(3, 5);
       Instant timestamp = Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(inputDate));
       UserModCommand expected = new UserModCommand(timestamp, "14","add",expectedSegments);
       UserModCommand umc = convertor.convert(input);
       
       assertEquals(expected, umc);
    }
    
    @Test(expected=InvalidArgumentException
            .class)
    public void testConvertorInputWithoutArguments() throws IOException, InvalidArgumentException {
       String input = inputDate+",14,add";
       Convertor convertor = new ConvertorImpl();
       convertor.convert(input);
    }
    
    @Test(expected=DateTimeParseException
            .class)
    public void testConvertorInputWithoutArguma() throws IOException, InvalidArgumentException {
       String input = "2016-12-11"+",14,add,incapsulate,run";
       Convertor convertor = new ConvertorImpl();
       convertor.convert(input);
    }
    
    
    
    

}
