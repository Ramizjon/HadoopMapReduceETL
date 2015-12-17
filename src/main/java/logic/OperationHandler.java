package logic;

import dataTO.UserModCommand;

public class OperationHandler {
	public void performOperationByType(UserModCommand command){
		if (command.getCommand().equals("true")){
			UserRepository.getInstance().addUserToTempQueue(command.getUserId(), command.getSegment());
		}
		else UserRepository.getInstance().removeUserFromHbase(command.getUserId(), command.getSegment());
	}
}
