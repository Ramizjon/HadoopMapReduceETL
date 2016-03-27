package com.unifier.nexusprovider;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.common.mapreduce.ReducerUserModCommand;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class NexusConvertorTestCase {

    @Test
    public void testConvertorWithValidInput() throws IOException {
        String input = "2011-12-03T10:15:30+01:00,14,add,generatedlink,closedtab";
        NexusConvertor convertor = new NexusConvertor();
        String timestamp = "2011-12-03T10:15:30+01:00";

        Map<String, String> segmentTimeStamps = ImmutableMap.of("generatedlink", timestamp, "closedtab", timestamp);
        ReducerUserModCommand expected = new ReducerUserModCommand("14", "add", segmentTimeStamps);

        List<ReducerUserModCommand> umc = convertor.convert(new AbstractMap.SimpleEntry<String, Mapper< LongWritable, Text, Void, GenericRecord >.Context>(input, null));

        assertEquals(expected, umc.get(0));
    }

    @Test(expected = InvalidArgumentException.class)
    public void testConvertorWithNotEnoughArguments() throws IOException {
        String input = "2011-12-03T10:15:30+01:00,14";
        NexusConvertor convertor = new NexusConvertor();
        String timestamp = "2011-12-03T10:15:30+01:00";

        Map<String, String> segmentTimeStamps = ImmutableMap.of("generatedlink", timestamp, "closedtab", timestamp);
        ReducerUserModCommand expected = new ReducerUserModCommand("14", "add", segmentTimeStamps);

        List<ReducerUserModCommand> umc = convertor.convert(new AbstractMap.SimpleEntry<String, Mapper< LongWritable, Text, Void, GenericRecord >.Context>(input, null));

        assertEquals(expected, umc.get(0));
    }


}
