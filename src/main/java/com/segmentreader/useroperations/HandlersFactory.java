package com.segmentreader.useroperations;

import java.util.HashMap;
import java.util.Map;

public class HandlersFactory {
	private Map<String, OperationHandler> handlers;
	HandlersCreator[] creators;
	String[] operationTypes;

	public HandlersFactory() {
		initHandlers();
	}

	private void initHandlers() {
		creators = new HandlersCreator[] { new AddHandlerCreator(),
				new DeleteHandlerCreator() };
		operationTypes = new String[] { "add", "delete" };
		handlers = new HashMap<String, OperationHandler>();
		for (int i = 0; i < creators.length; i++) {
			handlers.put(operationTypes[i], creators[i].createHandler());
		}
	}

	public Map<String, OperationHandler> getHandlers() {
		return handlers;
	}

}
