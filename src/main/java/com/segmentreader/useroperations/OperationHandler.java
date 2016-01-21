package com.segmentreader.useroperations;

import com.segmentreader.mapreduce.UserModCommand;

public interface OperationHandler {
	void handle(UserModCommand value);
}