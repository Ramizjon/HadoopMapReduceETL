package com.segmentreader.useroperations;

public class DeleteHandlerCreator extends HandlersCreator{

	@Override
	public OperationHandler createHandler() {
		return new DeleteOperationHandler();
	}
	
}
