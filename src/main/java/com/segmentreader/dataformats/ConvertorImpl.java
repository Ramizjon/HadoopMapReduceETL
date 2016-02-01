package com.segmentreader.dataformats;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.segmentreader.mapreduce.UserModCommand;

public class ConvertorImpl implements Convertor {

    public UserModCommand convert(String value) throws IOException {
        String[] arr = value.split(",");

        if (arr.length < 3) {
            throw new IOException("Validation failed: not enough arguments");
        }

        List segmentsList = Arrays.asList(arr).subList(2, arr.length);
        return new UserModCommand(arr[0], arr[1], segmentsList);
    }

}
