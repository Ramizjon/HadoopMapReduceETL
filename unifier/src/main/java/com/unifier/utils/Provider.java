package com.unifier.utils;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by ramizjon on 3/1/16.
 */
public abstract class Provider {

    public abstract boolean isOptional();
    public abstract Mapper<?, ?, Void, GenericRecord> getMapper();
}
