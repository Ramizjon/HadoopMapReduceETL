package com.segmentreader.dataformats;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.junit.Test;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.segmentreader.mapreduce.UserModCommand;

public class ConvertorImplTestCase {

    @Test
    public void testConvertorWithValidInput() throws IOException, ParseException {
       String input = "2014-08-22 15:02:51:580+12:15,14,add,generatedlink,closedtab";
       Convertor convertor = new ConvertorImpl();
       List<String> expectedSegments = Arrays.asList(input.split(",")).subList(3, 5);
       
       SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS+hh:mm");
       Date parsedTimeStamp = dateFormat.parse("2014-08-22 15:02:51:580+12:15");
       Timestamp timestamp = new Timestamp(parsedTimeStamp.getTime());
       UserModCommand expected = new UserModCommand(timestamp, "14","add",expectedSegments);
       UserModCommand umc = convertor.convert(input);
       
       assertEquals(expected, umc);
    }
    
    @Test(expected=InvalidArgumentException
            .class)
    public void testConvertorWithInvalidInput() throws IOException, InvalidArgumentException, ParseException {
       String input = "2014-08-22 15:02:51:580+12:15,14,add";
       Convertor convertor = new ConvertorImpl();
       convertor.convert(input);
    }

}
