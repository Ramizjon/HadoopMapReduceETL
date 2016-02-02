package com.segmentreader.dataformats;

import java.text.ParseException;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.segmentreader.mapreduce.UserModCommand;

public interface Convertor {
    public UserModCommand convert(String value) throws InvalidArgumentException, ParseException;
}
