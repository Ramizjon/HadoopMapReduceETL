package com.unifier.facebookprovider;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.common.mapreduce.MapperUserModCommand;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import utils.Convertor;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by admin on 3/7/16.
 */
public class FacebookConvertor implements Convertor<SimpleEntry<String, Mapper<LongWritable,Text,Void,GenericRecord>.Context>, List<MapperUserModCommand> > {

    @Override
    public List<MapperUserModCommand>  convert(SimpleEntry<String, Mapper<LongWritable,Text,Void,GenericRecord>.Context> inputTuple) {
        List<MapperUserModCommand> umcList = new ArrayList<>();
        String[] elements = inputTuple.getKey().split("/");
        if (elements.length < 3) {
            throw new InvalidArgumentException("Validation failed: not enough arguments");
        }

        String outputPath = FileOutputFormat.getOutputPath(new JobConf(inputTuple.getValue().getConfiguration())).toString();
        String[] pathArray = outputPath.split("/");
        String timestamp = pathArray[pathArray.length-3]+"T"+pathArray[pathArray.length-2]+":00:00";

        String[] segmentsToAdd = elements[1].split(",");
        String[] segmentsToRemove = elements[2].split(",");

        if (segmentsToAdd.length > 0) {
            ArrayList<String> segmentsToAddList = new ArrayList<>(Arrays.asList(segmentsToAdd));
            umcList.add(new MapperUserModCommand(timestamp, elements[0], "add", segmentsToAddList));
        }
        if (segmentsToRemove.length > 0) {
            ArrayList<String> segmentsToRemoveList = new ArrayList<>(Arrays.asList(segmentsToRemove));
            umcList.add(new MapperUserModCommand(timestamp, elements[0], "remove", segmentsToRemoveList));
        }
        return umcList;
    }
}
