package com.receiver.nexusprovider;

import com.receiver.mapreduce.AppContext;
import com.receiver.utils.Provider;
import lombok.Data;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapreduce.Mapper;

@Data
public class NexusProvider extends Provider {

    private final boolean isOptional = false;

    public Mapper<?, ?, Void, GenericRecord> getMapper() {
        return new AppContext.NexusUserSegmentsMapperImpl();
    }
}
