package com.aggregator.utils;


import com.common.mapreduce.MapperUserModCommand;
import org.apache.avro.generic.GenericRecord;
import utils.Convertor;

public class GenericRecordToMapperUserModCommandConvertor implements Convertor <GenericRecord, MapperUserModCommand> {

    @Override
    public MapperUserModCommand convert(GenericRecord value) {
        return null;
    }
}
