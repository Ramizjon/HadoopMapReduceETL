package com.unifier.facebookprovider;

import com.unifier.AppContext;
import com.unifier.utils.Provider;
import lombok.Data;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapreduce.Mapper;

@Data
public class FacebookProvider implements Provider {

    private final boolean isOptional = false;
    public Mapper<?, ?, Void, GenericRecord> getMapper() {
        return new AppContext.FacebookUserSegmentsMapperImpl();
    }
}
