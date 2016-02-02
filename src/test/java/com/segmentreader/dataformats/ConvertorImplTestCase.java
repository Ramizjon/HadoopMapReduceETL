package com.segmentreader.dataformats;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.segmentreader.mapreduce.UserModCommand;

public class ConvertorImplTestCase {

    @Test
    public void testConvertorWithValidInput() throws IOException, ParseException {
       String epoch = Instant.EPOCH.toString();
       String input = epoch+",14,add,generatedlink,closedtab";
       Convertor convertor = new ConvertorImpl();
       List<String> expectedSegments = Arrays.asList(input.split(",")).subList(3, 5);
       
       DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
       LocalDateTime localDateTime = LocalDateTime.parse(epoch, formatter);
       Instant timestamp = localDateTime.toInstant(ZoneOffset.UTC);
       UserModCommand expected = new UserModCommand(timestamp, "14","add",expectedSegments);
       UserModCommand umc = convertor.convert(input);
       
       assertEquals(expected, umc);
    }
    
    @Test(expected=InvalidArgumentException
            .class)
    public void testConvertorWithInvalidInput() throws IOException, InvalidArgumentException, ParseException {
       String input = Instant.EPOCH.toString()+",14,add";
       Convertor convertor = new ConvertorImpl();
       convertor.convert(input);
    }

}
