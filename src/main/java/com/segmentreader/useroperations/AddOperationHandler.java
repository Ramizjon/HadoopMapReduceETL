package com.segmentreader.useroperations;


import com.segmentreader.domain.UserRepository;
import com.segmentreader.domain.HBaseUserRepositoryImpl;
import com.segmentreader.mapreduce.UserModCommand;

public abstract class AddOperationHandler implements OperationHandler {

	UserRepository userRepository = getRepoInstance();
	
	@Override
	public void handle(UserModCommand value) {
		userRepository.addUser(value.getUserId(), value.getSegments());
	}

	protected abstract HBaseUserRepositoryImpl getRepoInstance();


}
