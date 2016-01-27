package com.segmentreader.useroperations;

import com.segmentreader.domain.UserRepositoryImpl;
import com.segmentreader.mapreduce.AppContext;

public class DeleteOperationHandlerImpl extends DeleteOperationHandler {

	@Override
	protected UserRepositoryImpl getRepoInstance() {
		return AppContext.getUserRepository();
	}

}
