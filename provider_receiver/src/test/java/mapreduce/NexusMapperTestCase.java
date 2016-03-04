package mapreduce;

import com.common.mapreduce.MapperUserModCommand;

import static org.mockito.Matchers.any;

import com.receiver.dataformats.Convertor;
import com.receiver.dataformats.ConvertorImpl;
import com.receiver.mapreduce.AppContext;
import com.receiver.nexusprovider.NexusUserSegmentsMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.receiver.mapreduce.AppContext.NexusUserSegmentsMapperImpl;

import static org.mockito.Mockito.*;

public class NexusMapperTestCase {

    private NexusUserSegmentsMapper createInstance(
            Convertor convertor) {
        return new NexusUserSegmentsMapper() {

            @Override
            protected Convertor getConvertor() {
                return convertor;
            }
        };
    }

    @Test
    public void testMapperWithValidArgs() throws IOException, InterruptedException {
        //prepare stage
        Mapper.Context context = mock(Mapper.Context.class);
        Counter mapRedCounter = mock(Counter.class);
        Convertor convertor = new ConvertorImpl();
        String timestamp = "2011-12-03T10:15:30+01:00";
        MapperUserModCommand userMod = new MapperUserModCommand(timestamp, "user22", "delete", new ArrayList<>(Arrays.asList("iphone")));
        Schema schema = ReflectData.get().getSchema(MapperUserModCommand.class);
        GenericRecord record = new GenericData.Record(schema);
        record.put("timestamp", userMod.getTimestamp());
        record.put("userId", userMod.getUserId());
        record.put("command", userMod.getCommand());
        record.put("segments", userMod.getSegments());
        String input = "2011-12-03T10:15:30+01:00,user22,delete,iphone";
        NexusUserSegmentsMapper testMapper = createInstance(convertor);
        when(context.getCounter("nexus_provider_reader", "nexus_map_counter")).thenReturn(mapRedCounter);

        //act stage
        testMapper.map(null, new Text(input), context);

        //asserts stage
        verify(context, times(1)).write(null, record);
    }
}
