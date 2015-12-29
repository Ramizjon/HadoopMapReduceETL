package logic;
import mapreduce.UserModCommand;

public class OperationHandler {
	public void performOperationByType(UserModCommand command){
		if (command.getCommand().equals("add")){
			UserRepository.getInstance().addUserToTempQueue(command.getUserId(), command.getSegments());
		}
		else if (command.getCommand().equals("delete"))
			UserRepository.getInstance().removeUserFromHbase("Id"+String.valueOf(command.getUserId()), command.getSegments());
		}
}
