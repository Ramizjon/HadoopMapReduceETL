package com.segmentreader.useroperations;


import java.io.IOException;

import com.segmentreader.domain.UserRepository;
import com.segmentreader.mapreduce.UserModCommand;

public abstract class AddOperationHandler implements OperationHandler {

	UserRepository userRepository = getRepoInstance();
	
	@Override
	public void handle(UserModCommand value) throws IOException {
		userRepository.addUser(value);
	}
	
	protected abstract UserRepository getRepoInstance();
}
