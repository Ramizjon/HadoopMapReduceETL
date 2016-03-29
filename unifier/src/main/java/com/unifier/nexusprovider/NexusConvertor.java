package com.unifier.nexusprovider;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.common.mapreduce.ReducerUserModCommand;
import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.Convertor;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class NexusConvertor implements Convertor<AbstractMap.SimpleEntry<String, Mapper<LongWritable, Text, Void, GenericRecord>.Context>, List<ReducerUserModCommand>> {

    @Override
    public List<ReducerUserModCommand> convert(AbstractMap.SimpleEntry<String, Mapper<LongWritable, Text, Void, GenericRecord>.Context> inputTuple) {
        String value = inputTuple.getKey();
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

        return Arrays.asList(new ReducerUserModCommand(arr[1], arr[2], segmentsMap));
    }
}
