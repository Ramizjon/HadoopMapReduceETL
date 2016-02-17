package com.segmentreader.useroperations;

import java.io.IOException;

import com.segmentreader.domain.UserRepository;
import com.segmentreader.mapreduce.MapperUserModCommand;
import com.segmentreader.mapreduce.ReducerUserModCommand;

public abstract class DeleteOperationHandler implements OperationHandler{

	UserRepository instance = getRepoInstance();
	
	@Override
	public void handle(ReducerUserModCommand value) throws IOException {
	    instance.removeUser(value.getUserId());
	}
	
	protected abstract UserRepository getRepoInstance();

}
