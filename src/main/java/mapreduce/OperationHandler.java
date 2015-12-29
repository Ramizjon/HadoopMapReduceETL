
package mapreduce;
import domain.UserRepository;
public class OperationHandler {
	public void performOperationByType(mapreduce.UserModCommand value){
		if (value.getCommand().equals("add")){
			UserRepository.getInstance().addUserToTempQueue(value.getUserId(), value.getSegments());
		}
		else if (value.getCommand().equals("delete"))
			UserRepository.getInstance().removeUserFromHbase("Id"+String.valueOf(value.getUserId()), value.getSegments());
		}
}
