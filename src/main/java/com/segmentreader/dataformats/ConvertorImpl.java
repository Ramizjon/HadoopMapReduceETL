package com.segmentreader.dataformats;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.segmentreader.mapreduce.UserModCommand;

public class ConvertorImpl implements Convertor {

    public UserModCommand convert(String value) throws InvalidArgumentException {
        String[] arr = value.split(",");

        if (arr.length < 4) {
            throw new InvalidArgumentException("Validation failed: not enough arguments");
        }

        ArrayList<String> segmentsList = new ArrayList<>(Arrays.asList(arr).subList(3, arr.length));
        Instant timestamp = parseDateToInstant(arr[0]);
        return new UserModCommand(timestamp, arr[1], arr[2], segmentsList);
    }
    
    private Instant parseDateToInstant (String date) {
        return Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(date));
     }

}
