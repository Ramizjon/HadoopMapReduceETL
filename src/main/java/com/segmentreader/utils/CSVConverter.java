package com.segmentreader.utils;


import com.segmentreader.mapreduce.ReducerUserModCommand;

import java.util.StringJoiner;

public class CSVConverter {

    /*public static String convertUserModToCSV (ReducerUserModCommand rumc) {
        StringJoiner sj = new StringJoiner(",");
        rumc.getSegmentTimestamps().getKey().forEach(s -> sj.add(s));
        return rumc.getSegmentTimestamps().getValue().toString() + ","
                + rumc.getUserId() + ","
                + rumc.getCommand() + ","
                + sj.toString();
    }*/
}
