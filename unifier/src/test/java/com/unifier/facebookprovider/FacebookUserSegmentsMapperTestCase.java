package com.unifier.facebookprovider;

import com.common.mapreduce.MapperUserModCommand;
import com.unifier.dataformats.Convertor;
import com.unifier.dataformats.ConvertorImpl;
import com.unifier.nexusprovider.NexusUserSegmentsMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


public class FacebookUserSegmentsMapperTestCase {

        private FacebookUserSegmentsMapper createInstance(Convertor convertor) {
            return new FacebookUserSegmentsMapper() {
                @Override
                protected Convertor getConvertor() {
                    return convertor;
                }
            };
        }

        @Test
        public void testNexusUserSegmentsMapper() throws IOException, InterruptedException {
            //prepare stage
            Mapper.Context context = mock(Mapper.Context.class);
            Counter mapRedCounter = mock(Counter.class);
            Convertor convertor = mock(Convertor.class);
            String timestamp = "2011-12-03T10:15:30+01:00";
            MapperUserModCommand userModAdd = new MapperUserModCommand(timestamp, "11", "add",
                    new ArrayList<>(Arrays.asList("link_clicked", "link_hovered")));
            Schema schema = ReflectData.get().getSchema(MapperUserModCommand.class);
            GenericRecord recordAdd = new GenericData.Record(schema);
            recordAdd.put("timestamp", userModAdd.getTimestamp());
            recordAdd.put("userId", userModAdd.getUserId());
            recordAdd.put("command", userModAdd.getCommand());
            recordAdd.put("segments", userModAdd.getSegments());

            MapperUserModCommand userModDelete = new MapperUserModCommand(timestamp, "11", "delete",
                    new ArrayList<>(Arrays.asList("page_closed", "page_opened")));
            GenericRecord recordDelete = new GenericData.Record(schema);
            recordDelete.put("timestamp", userModAdd.getTimestamp());
            recordDelete.put("userId", userModAdd.getUserId());
            recordDelete.put("command", userModAdd.getCommand());
            recordDelete.put("segments", userModAdd.getSegments());

            List<MapperUserModCommand> cmdList = Arrays.asList(userModAdd, userModDelete);

            String input = "11/link_clicked,link_hovered/page_closed,page_opened";
            FacebookUserSegmentsMapper testMapper = createInstance(convertor);
            when(context.getCounter("facebook_provider_reader", "facebook_map_counter")).thenReturn(mapRedCounter);
            when(convertor.convertFacebookUMC(input, context)).thenReturn(cmdList);

            //act stage
            testMapper.map(null, new Text(input), context);

            //asserts stage
            verify(mapRedCounter, times(1)).increment(1);
            verify(context, times(1)).write(null, recordAdd);
            verify(context, times(1)).write(null, recordDelete);

        }
    }


