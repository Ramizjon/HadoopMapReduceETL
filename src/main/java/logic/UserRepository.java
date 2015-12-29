package logic;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import domain.User;


public class UserRepository {
	LinkedList <User> cachedList;
	private final int BUFFER_SIZE = 4;
	private static UserRepository instance;
	private Configuration config;
	private  HTable hTable;
	
	//TODO approach will be changed
	public static UserRepository getInstance (){
		if (instance == null){
			instance  = new UserRepository();
		}
		return instance;
	}
	
	public UserRepository (){
		cachedList = new LinkedList<User>();
		config = HBaseConfiguration.create();
		try {
			hTable = new HTable(config, "users");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void addUserToTempQueue(int userId, String segment){
		cachedList.add(new User(userId, segment));
		this.checkForBulk();
	}
	
	public void checkForBulk(){
		if (cachedList.size()==BUFFER_SIZE){
			this.addUsersToHbase();
			cachedList.clear();
		}
	}
	
	//just for debug
	public String print(){
		StringBuilder sb = new StringBuilder();
		for (User user: cachedList){
			sb.append(user.getSegment() + " ");
		}
		return ("------------\n" + sb.toString());
	}
	
	
	public void addUsersToHbase (){
		Put put = null;
		for (User u: cachedList){
			put = new Put(Bytes.toBytes("Id" + String.valueOf(u.getUserId())));
			put.add(Bytes.toBytes("general"),Bytes.toBytes("segment"),Bytes.toBytes(u.getSegment()));
			try {
				hTable.put(put);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	public void removeUserFromHbase(String rowId, String segment){
		Delete delete = new Delete (Bytes.toBytes(rowId));
		delete.deleteColumns(Bytes.toBytes("general"), Bytes.toBytes("segment"));
		try {
			hTable.delete(delete);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//TODO implement receiving User from HBase by id
	public User getUser(int userId){
		return new User(userId, null);
	}
	
}
