package com.segmentreader.dataformats;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;

import org.junit.Test;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.segmentreader.mapreduce.UserModCommand;

public class ConvertorImplTestCase {

    @Test
    public void testConvertorWithValidInput() throws IOException {
       String input = "2011-12-03T10:15:30+01:00,14,add,generatedlink,closedtab";
       Convertor convertor = new ConvertorImpl();
       Instant timestamp = parseDateToInstant("2011-12-03T10:15:30+01:00");
       UserModCommand expected = new UserModCommand(timestamp, "14","add", Arrays.asList("generatedlink", "closedtab"));
       UserModCommand umc = convertor.convert(input);
       
       assertEquals(expected, umc);
    }
    
    @Test(expected=InvalidArgumentException.class)
    public void testConvertorInputWithoutSegmentsList() throws IOException, InvalidArgumentException {
       String input = "2011-12-03T10:15:30+01:00,14,add";
       Convertor convertor = new ConvertorImpl();
       convertor.convert(input);
    }
    
    @Test(expected=DateTimeParseException.class)
    public void testConvertorInputWithWrongDateFormat() throws IOException, InvalidArgumentException {
       String input = "2016-12-11,14,add,incapsulate,run";
       Convertor convertor = new ConvertorImpl();
       convertor.convert(input);
    }
    
    private Instant parseDateToInstant (String date) {
       return Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(date));
    }
   
}
