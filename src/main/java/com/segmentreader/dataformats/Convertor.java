package com.segmentreader.dataformats;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.segmentreader.mapreduce.UserModCommand;

public interface Convertor {
    public UserModCommand convert(String value) throws InvalidArgumentException;
}
