package com.receiver.dataformats;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.common.mapreduce.MapperUserModCommand;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper.Context;

@Slf4j
public class ConvertorImpl implements Convertor {

    public MapperUserModCommand convertNexusUMC(String value) throws InvalidArgumentException {
        String[] arr = value.split(",");
        if (arr.length < 4) {
            throw new InvalidArgumentException("Validation failed: not enough arguments");
        }

        ArrayList<String> segmentsList = new ArrayList<>(Arrays.asList(arr).subList(3, arr.length));
        return new MapperUserModCommand(arr[0], arr[1], arr[2], segmentsList);
    }

    public List<MapperUserModCommand> convertFacebookUMC(String value, Context context) throws InvalidArgumentException {
        List<MapperUserModCommand> umcList = new ArrayList<>();
        String[] elements = value.split("/");
        if (elements.length < 3) {
            throw new InvalidArgumentException("Validation failed: not enough arguments");
        }

        String outputPath = FileOutputFormat.getOutputPath(new JobConf(context.getConfiguration())).toString();
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

    private Instant parseDateToInstant (String date) {
        return Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(date));
    }

}
