package logic;

import java.util.LinkedList;

import org.apache.zookeeper.AsyncCallback.StatCallback;

import domain.User;

public class UserRepository {
	LinkedList <User> cachedList;
	private final int BUFFER_SIZE = 200;
	private static UserRepository instance;
	private int userCounter;
	
	//TODO approach will be changed
	public static UserRepository getInstance (){
		if (instance == null){
			instance  = new UserRepository();
		}
		return instance;
	}
	
	public UserRepository (){
		cachedList = new LinkedList<User>();
		userCounter = 0;
	}
	
	public void addUserToTempQueue(int userId, String segment){
		cachedList.add(new User(userId, segment));
		this.userCounter++;
		this.checkForBulk();
	}
	
	public void checkForBulk(){
		if (userCounter==BUFFER_SIZE){
			this.addUsersToHbase();
			cachedList.clear();
			userCounter = 0;
		}
	}
	
	//just for debug
	public void print(){
		System.out.println(this.cachedList);
	}
	
	//TODO implement adding to real HBase table
	public void addUsersToHbase (){}
	
	//TODO implement remove from HBase
	public void removeUserFromHbase(int userId, String segment){}
	
	//TODO implement receiving User from HBase by id
	public User getUser(int userId){
		return new User(userId, null);
	}
	
}
