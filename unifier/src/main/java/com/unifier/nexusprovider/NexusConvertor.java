package com.unifier.nexusprovider;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.common.mapreduce.MapperUserModCommand;
import utils.Convertor;

import java.util.ArrayList;
import java.util.Arrays;


public class NexusConvertor implements Convertor<String, MapperUserModCommand> {

    @Override
    public MapperUserModCommand convert(String value) {
        String[] arr = value.split(",");
        if (arr.length < 4) {
            throw new InvalidArgumentException("Validation failed: not enough arguments");
        }

        ArrayList<String> segmentsList = new ArrayList<>(Arrays.asList(arr).subList(3, arr.length));
        return new MapperUserModCommand(arr[0], arr[1], arr[2], segmentsList);
    }
}
