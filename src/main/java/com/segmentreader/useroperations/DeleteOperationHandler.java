package com.segmentreader.useroperations;

import com.segmentreader.domain.UserRepository;
import com.segmentreader.domain.HBaseUserRepositoryImpl;
import com.segmentreader.mapreduce.UserModCommand;

public abstract class DeleteOperationHandler implements OperationHandler{

	UserRepository instance = getRepoInstance();
	
	@Override
	public void handle(UserModCommand value) {
	    instance.removeUser("Id"+String.valueOf(value.getUserId()), value.getSegments());
	}
	
	protected abstract HBaseUserRepositoryImpl getRepoInstance();

}
