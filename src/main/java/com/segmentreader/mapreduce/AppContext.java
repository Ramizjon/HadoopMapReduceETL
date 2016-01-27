package com.segmentreader.mapreduce;

import java.util.HashMap;
import java.util.Map;

import com.segmentreader.domain.UserRepositoryImpl;
import com.segmentreader.useroperations.AddOperationHandler;
import com.segmentreader.useroperations.DeleteOperationHandler;
import com.segmentreader.useroperations.OperationHandler;

public class AppContext {

	private static UserRepositoryImpl userRepository = new UserRepositoryImpl();

	public static class Mapper extends LineMapper {
		Map<String, OperationHandler> getHandlers() {
			Map<String, OperationHandler> handlersMap = new HashMap<String, OperationHandler>();
			handlersMap.put(OperationHandler.ADD_OPERATION,  new AppContext.AddOperationHandlerImpl());
			handlersMap.put(OperationHandler.DELETE_OPERATION, new AppContext.DeleteOperationHandlerImpl());
			return handlersMap;
		}
	}
	
 public static class AddOperationHandlerImpl extends AddOperationHandler{

		@Override
		protected UserRepositoryImpl getRepoInstance() {
			return userRepository;
		}

	}
	
	public static class DeleteOperationHandlerImpl extends DeleteOperationHandler {

		@Override
		protected UserRepositoryImpl getRepoInstance() {
			return userRepository;
		}

	}


	public static UserRepositoryImpl getUserRepository() {
		return userRepository;
	}
}
