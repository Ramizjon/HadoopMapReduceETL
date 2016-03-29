package com.unifier.nexusprovider;

import com.unifier.AppContext;
import com.unifier.Provider;
import lombok.Data;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapreduce.Mapper;

@Data
public class NexusProvider implements Provider {

    private final boolean isOptional = false;

    public Mapper<?, ?, Void, GenericRecord> getMapper() {
        return new AppContext.NexusUserSegmentsMapperImpl();
    }
}
