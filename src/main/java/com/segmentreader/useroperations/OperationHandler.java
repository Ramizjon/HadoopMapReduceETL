package com.segmentreader.useroperations;

import com.segmentreader.mapreduce.UserModCommand;

public interface OperationHandler {
	void handle(UserModCommand value);
	public static final String ADD_OPERATION = "add";
	public  static final String  DELETE_OPERATION = "delete";
}