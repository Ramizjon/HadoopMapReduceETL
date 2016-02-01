package com.segmentreader.dataformats;

import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.segmentreader.mapreduce.UserModCommand;

public class ConvertorImpl implements Convertor {

    public UserModCommand convert(String value) throws InvalidArgumentException {
        String[] arr = value.split(",");

        if (arr.length < 3) {
            throw new InvalidArgumentException("Validation failed: not enough arguments");
        }

        List<String> segmentsList = Arrays.asList(arr).subList(2, arr.length);
        return new UserModCommand(arr[0], arr[1], segmentsList);
    }

}
