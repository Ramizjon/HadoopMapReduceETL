package com.segmentreader.useroperations;

import com.segmentreader.domain.UserRepository;
import com.segmentreader.mapreduce.UserModCommand;

public class DeleteOperationHandler implements OperationHandler{

	@Override
	public void handle(UserModCommand value) {
		UserRepository.getInstance().removeUserFromHbase("Id"+String.valueOf(value.getUserId()), value.getSegments());
	}

}
