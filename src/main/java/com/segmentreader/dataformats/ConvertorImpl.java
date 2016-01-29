package com.segmentreader.dataformats;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.io.Text;

import com.segmentreader.mapreduce.UserModCommand;

public class ConvertorImpl implements Convertor {

    public UserModCommand convert(String value) throws IOException {
        String[] arr = value.split(",");

        if (arr.length < 3) {
            throw new IOException("Validation failed: not enough arguments");
        }

        LinkedList<String> segmentsList = new LinkedList<String>();
        for (int i = 2; i < arr.length; i++) {
            segmentsList.add(arr[i]);
        }
        return new UserModCommand(arr[0], arr[1], segmentsList);
    }

}
