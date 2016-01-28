package com.segmentreader.useroperations;

import java.io.IOException;

import com.segmentreader.domain.UserRepository;
import com.segmentreader.domain.HBaseUserRepositoryImpl;
import com.segmentreader.mapreduce.UserModCommand;

public abstract class DeleteOperationHandler implements OperationHandler{

	UserRepository instance = getRepoInstance();
	
	@Override
	public void handle(UserModCommand value) throws IOException {
	    instance.removeUser(value.getUserId());
	}
	
	protected abstract HBaseUserRepositoryImpl getRepoInstance();

}
