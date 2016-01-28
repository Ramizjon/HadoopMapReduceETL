package com.segmentreader.useroperations;

import java.io.IOException;

import com.segmentreader.mapreduce.UserModCommand;

public interface OperationHandler {
    public static final String ADD_OPERATION = "add";
    public  static final String  DELETE_OPERATION = "delete";
    
    /**
     * Handles user operation regarding it's type
     * @param value
     */
	void handle(UserModCommand value) throws IOException;
}