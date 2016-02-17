package com.segmentreader.useroperations;

import java.io.IOException;

import com.segmentreader.mapreduce.MapperUserModCommand;
import com.segmentreader.mapreduce.ReducerUserModCommand;

public interface OperationHandler {
    public static final String ADD_OPERATION = "add";
    public  static final String  DELETE_OPERATION = "delete";
    
    /**
     * Handles user operation regarding it's type
     * @param value
     */
	void handle(ReducerUserModCommand value) throws IOException;
}