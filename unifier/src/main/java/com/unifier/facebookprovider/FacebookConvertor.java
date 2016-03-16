package com.unifier.facebookprovider;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.common.mapreduce.MapperUserModCommand;
import com.common.mapreduce.ReducerUserModCommand;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import utils.Convertor;

import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.stream.Collectors;


public class FacebookConvertor implements Convertor<SimpleEntry<String, Mapper<LongWritable, Text, Void, GenericRecord>.Context>, List<ReducerUserModCommand>> {

    @Override
    public List<ReducerUserModCommand> convert(SimpleEntry<String, Mapper<LongWritable, Text, Void, GenericRecord>.Context> inputTuple) {
        List<ReducerUserModCommand> umcList = new ArrayList<>();
        String[] elements = inputTuple.getKey().split("/");
        if (elements.length < 3) {
            throw new InvalidArgumentException("Validation failed: not enough arguments");
        }

        String outputPath = FileOutputFormat.getOutputPath(new JobConf(inputTuple.getValue().getConfiguration())).toString();
        String[] pathArray = outputPath.split("/");
        String timestamp = pathArray[pathArray.length - 3] + "T" + pathArray[pathArray.length - 2] + ":00:00";

        String[] segmentsToAdd = elements[1].split(",");
        String[] segmentsToDelete = elements[2].split(",");

        if (segmentsToAdd.length > 0) {
            Map<String, String> segmentsToAddMap = Arrays.asList(segmentsToAdd)
                    .stream()
                    .collect(Collectors.toMap(
                            e -> e,
                            e -> timestamp
                    ));
            umcList.add(new ReducerUserModCommand(elements[0], "add", segmentsToAddMap));
        }
        if (segmentsToDelete.length > 0) {
            Map<String, String> segmentsToDeleteMap = Arrays.asList(segmentsToDelete)
                    .stream()
                    .collect(Collectors.toMap(
                            e -> e,
                            e -> timestamp
                    ));
            umcList.add(new ReducerUserModCommand(elements[0], "add", segmentsToDeleteMap));
        }
        return umcList;
    }


}
