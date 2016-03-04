package com.receiver.dataformats;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.common.mapreduce.MapperUserModCommand;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.List;

public interface Convertor {
    public MapperUserModCommand convertNexusUMC(String value) throws InvalidArgumentException;
    public List<MapperUserModCommand> convertFacebookUMC(String value, Mapper.Context context) throws InvalidArgumentException;
}
