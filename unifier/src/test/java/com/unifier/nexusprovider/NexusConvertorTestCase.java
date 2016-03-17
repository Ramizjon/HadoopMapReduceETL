package com.unifier.nexusprovider;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.common.mapreduce.ReducerUserModCommand;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.io.IOException;
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

        ReducerUserModCommand umc = convertor.convert(input);

        assertEquals(expected, umc);
    }

    @Test(expected = InvalidArgumentException.class)
    public void testConvertorWithNotEnoughArguments() throws IOException {
        String input = "2011-12-03T10:15:30+01:00,14";
        NexusConvertor convertor = new NexusConvertor();
        String timestamp = "2011-12-03T10:15:30+01:00";

        Map<String, String> segmentTimeStamps = ImmutableMap.of("generatedlink", timestamp, "closedtab", timestamp);
        ReducerUserModCommand expected = new ReducerUserModCommand("14", "add", segmentTimeStamps);

        ReducerUserModCommand umc = convertor.convert(input);

        assertEquals(expected, umc);
    }


}
