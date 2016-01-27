package com.segmentreader.useroperations;

import com.segmentreader.domain.UserRepositoryImpl;
import com.segmentreader.mapreduce.AppContext;

public class AddOperationHandlerImpl extends AddOperationHandler{

	@Override
	protected UserRepositoryImpl getRepoInstance() {
		return AppContext.getUserRepository();
	}

}
