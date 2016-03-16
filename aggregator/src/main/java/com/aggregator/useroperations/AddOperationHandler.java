package com.aggregator.useroperations;


import java.io.IOException;

import com.aggregator.domain.UserRepository;
import com.common.mapreduce.ReducerUserModCommand;

public abstract class AddOperationHandler implements OperationHandler {

	UserRepository userRepository = getRepoInstance();
	
	@Override
	public void handle(ReducerUserModCommand value) throws IOException {
		userRepository.addUser(value);
	}
	
	protected abstract UserRepository getRepoInstance();
}
