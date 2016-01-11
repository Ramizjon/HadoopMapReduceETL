package com.segmentreader.useroperations;

public class AddHandlerCreator extends HandlersCreator {

	@Override
	public OperationHandler createHandler() {
		return new AddOperationHandler();
	}
	
}
