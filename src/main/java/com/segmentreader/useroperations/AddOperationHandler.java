package com.segmentreader.useroperations;


import com.segmentreader.domain.UserRepositoryImpl;
import com.segmentreader.mapreduce.UserModCommand;

public abstract class AddOperationHandler implements OperationHandler {

	UserRepositoryImpl userRepository = getRepoInstance();
	
	@Override
	public void handle(UserModCommand value) {
		userRepository.addUserToTempQueue(value.getUserId(), value.getSegments());
	}

	protected abstract UserRepositoryImpl getRepoInstance();


}
