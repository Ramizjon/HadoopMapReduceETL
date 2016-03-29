package com.aggregator.useroperations;

import com.common.mapreduce.ReducerUserModCommand;

import java.io.IOException;

public interface OperationHandler {
    public static final String ADD_OPERATION = "add";
    public  static final String  DELETE_OPERATION = "delete";
    
    /**
     * Handles user operation regarding it's type
     * @param value
     */
	void handle(ReducerUserModCommand value) throws IOException;
}