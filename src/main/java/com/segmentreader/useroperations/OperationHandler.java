package com.segmentreader.useroperations;

import com.segmentreader.domain.UserRepository;
import com.segmentreader.mapreduce.*;

public interface OperationHandler {
	void handle(UserModCommand value);
	
	/*
	public void performOperationByType(UserModCommand value){
		if (value.getCommand().equals("add")){
			UserRepository.getInstance().addUserToTempQueue(value.getUserId(), value.getSegments());
		}
		else if (value.getCommand().equals("delete"))
			UserRepository.getInstance().removeUserFromHbase("Id"+String.valueOf(value.getUserId()), value.getSegments());
		}*/
}
