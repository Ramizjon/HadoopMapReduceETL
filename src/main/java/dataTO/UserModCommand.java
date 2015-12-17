package dataTO;

import parquet.schema.GroupType;

public class UserModCommand {
	int userId;
	String command;
	String segment;
	
	public UserModCommand(int userId, String command, String segment) {
		this.userId = userId;
		this.command = command;
		this.segment = segment;
	}
	
	public UserModCommand(){
		
	}
	
	public int getUserId() {
		return userId;
	}
	public void setUserId(int userId) {
		this.userId = userId;
	}
	public String getCommand() {
		return command;
	}
	public void setCommand(String command) {
		this.command = command;
	}
	public String getSegment() {
		return segment;
	}
	public void setSegment(String segment) {
		this.segment = segment;
	}
	
	
}
