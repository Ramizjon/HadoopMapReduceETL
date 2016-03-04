package com.aggregator.useroperations;

import java.io.IOException;

import com.aggregator.mapreduce.ReducerUserModCommand;

public interface OperationHandler {
    public static final String ADD_OPERATION = "add";
    public  static final String  DELETE_OPERATION = "delete";
    
    /**
     * Handles user operation regarding it's type
     * @param value
     */
	void handle(ReducerUserModCommand value) throws IOException;
}