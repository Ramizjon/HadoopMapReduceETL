package com.segmentreader.utils;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class TestHelper<T>{

    public ArrayList<T> toArrayList (List<T> inputList){
        return new ArrayList<T>(inputList);
    }

    public Instant parseDateToInstant (String date) {
        return Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(date));
    }

}
