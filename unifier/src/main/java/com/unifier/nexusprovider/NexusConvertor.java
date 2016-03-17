package com.unifier.nexusprovider;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.common.mapreduce.ReducerUserModCommand;
import utils.Convertor;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;


public class NexusConvertor implements Convertor<String, ReducerUserModCommand> {

    @Override
    public ReducerUserModCommand convert(String value) {
        String[] arr = value.split(",");
        if (arr.length < 4) {
            throw new InvalidArgumentException("Validation failed: not enough arguments");
        }

        Map<String, String> segmentsMap = Arrays.asList(arr).subList(3, arr.length)
                .stream()
                .collect(Collectors.toMap(
                        e -> e,
                        e -> arr[0]
                ));

        return new ReducerUserModCommand(arr[1], arr[2], segmentsMap);
    }
}
