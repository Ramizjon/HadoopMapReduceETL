package com.segmentreader.useroperations;


import java.io.IOException;

import com.segmentreader.domain.User;
import com.segmentreader.domain.UserRepository;
import com.segmentreader.mapreduce.UserModCommand;

public abstract class AddOperationHandler implements OperationHandler {

	UserRepository userRepository = getRepoInstance();
	
	@Override
	public void handle(UserModCommand value) throws IOException {
		userRepository.addUser(new User(value.getTimestamp(), value.getUserId(), value.getSegments()));
	}
	
	protected abstract UserRepository getRepoInstance();
}
