<<<<<<< HEAD:src/main/java/mapreduce/OperationHandler.java
package mapreduce;
import domain.UserRepository;
=======
package logic;
import dataTO.UserModCommand;
>>>>>>> b3be5d4... modified input value for hbase cell. Now, it's timestamp. Added support for multiple segments per one input line (list is used):src/main/java/logic/OperationHandler.java

public class OperationHandler {
	public void performOperationByType(UserModCommand command){
		if (command.getCommand().equals("add")){
			UserRepository.getInstance().addUserToTempQueue(command.getUserId(), command.getSegments());
		}
		else if (command.getCommand().equals("delete"))
			UserRepository.getInstance().removeUserFromHbase("Id"+String.valueOf(command.getUserId()), command.getSegments());
		}
}
