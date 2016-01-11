package com.segmentreader.useroperations;

import com.segmentreader.domain.UserRepository;
import com.segmentreader.mapreduce.UserModCommand;

public class AddOperationHandler implements OperationHandler {

	@Override
	public void handle(UserModCommand value) {
		UserRepository.getInstance().addUserToTempQueue(value.getUserId(), value.getSegments());
	}

}
