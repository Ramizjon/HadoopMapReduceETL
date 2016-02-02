package com.segmentreader.dataformats;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.segmentreader.mapreduce.UserModCommand;

public class ConvertorImpl implements Convertor {

    public UserModCommand convert(String value) throws InvalidArgumentException, ParseException {
        String[] arr = value.split(",");

        if (arr.length < 4) {
            throw new InvalidArgumentException("Validation failed: not enough arguments");
        }

        List<String> segmentsList = Arrays.asList(arr).subList(3, arr.length);
        
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS+hh:mm");
        Date parsedTimeStamp = dateFormat.parse(arr[0]);
        Timestamp timestamp = new Timestamp(parsedTimeStamp.getTime());
        return new UserModCommand(timestamp, arr[1], arr[2], segmentsList);
    }

}
