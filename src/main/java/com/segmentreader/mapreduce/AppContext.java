package com.segmentreader.mapreduce;

import java.util.HashMap;
import java.util.Map;

import com.segmentreader.domain.UserRepository;
import com.segmentreader.domain.UserRepositoryImpl;
import com.segmentreader.useroperations.AddOperationHandlerImpl;
import com.segmentreader.useroperations.DeleteOperationHandlerImpl;
import com.segmentreader.useroperations.OperationHandler;

public class AppContext {

	private static UserRepositoryImpl userRepository = new UserRepositoryImpl();

	public static class Mapper extends LineMapper {
		Map<String, OperationHandler> getHandlers() {
			OperationHandler[] handlersArr = new OperationHandler[] {
					new AddOperationHandlerImpl(),
					new DeleteOperationHandlerImpl() };
			String[] operationTypes = new String[] { "add", "delete" };
			Map<String, OperationHandler> handlersMap = new HashMap<String, OperationHandler>();
			for (int i = 0; i < handlersArr.length; i++) {
				handlersMap.put(operationTypes[i], handlersArr[i]);
			}
			return handlersMap;
		}
	}

	public static UserRepositoryImpl getUserRepository() {
		return userRepository;
	}
}
