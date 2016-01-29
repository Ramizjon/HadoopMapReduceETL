package com.segmentreader.dataformats;

import java.io.IOException;

import com.segmentreader.mapreduce.UserModCommand;

public interface Convertor {
    public UserModCommand convert(String value) throws IOException;
}
