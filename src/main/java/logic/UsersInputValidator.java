package logic;

import java.util.LinkedList;

public class UsersInputValidator {
	public LinkedList<User> validateInput (LinkedList<User> userList){
		LinkedList<User> newList = new LinkedList<User>();
		for (User user: userList){
			if (user.getOperationKeyValuePair().getKey()){
				newList.add(user);
			}
		}
		return newList;
	}	
}
