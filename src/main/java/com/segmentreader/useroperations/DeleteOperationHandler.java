package com.segmentreader.useroperations;

import com.segmentreader.domain.UserRepositoryImpl;
import com.segmentreader.mapreduce.UserModCommand;

public abstract class DeleteOperationHandler implements OperationHandler{

	UserRepositoryImpl instance = getRepoInstance();
	
	@Override
	public void handle(UserModCommand value) {
	    instance.removeUser("Id"+String.valueOf(value.getUserId()), value.getSegments());
	}
	
	protected abstract UserRepositoryImpl getRepoInstance();

}
