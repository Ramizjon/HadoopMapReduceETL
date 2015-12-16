package logic;

import java.util.LinkedList;
import java.util.List;

public class HBaseCreator {
	List<User> usersTable;
	
	public HBaseCreator (){
		this.createHBaseTable("/path/here");
	}
	
	public void createHBaseTable (String path){
		//table simulation
		usersTable = new LinkedList<User>();
	}
	public  void addUserIntoHBaseTable (User user){
		usersTable.add(user);
	}
	
	public void print(){
		System.out.println(usersTable.toString());
	}
	
}
