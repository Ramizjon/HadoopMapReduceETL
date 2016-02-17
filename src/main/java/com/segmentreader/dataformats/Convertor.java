package com.segmentreader.dataformats;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.segmentreader.mapreduce.MapperUserModCommand;

public interface Convertor {
    public MapperUserModCommand convert(String value) throws InvalidArgumentException;
}
