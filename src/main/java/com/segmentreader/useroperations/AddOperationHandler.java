package com.segmentreader.useroperations;


import java.io.IOException;

import com.segmentreader.domain.UserRepository;
import com.segmentreader.mapreduce.MapperUserModCommand;
import com.segmentreader.mapreduce.ReducerUserModCommand;

public abstract class AddOperationHandler implements OperationHandler {

	UserRepository userRepository = getRepoInstance();
	
	@Override
	public void handle(ReducerUserModCommand value) throws IOException {
		userRepository.addUser(value);
	}
	
	protected abstract UserRepository getRepoInstance();
}
