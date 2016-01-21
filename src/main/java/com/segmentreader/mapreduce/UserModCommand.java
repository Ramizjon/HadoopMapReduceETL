package com.segmentreader.mapreduce;

import java.util.LinkedList;

public class UserModCommand {
	int userId;
	String command;
	LinkedList<String> segments;
	
	public UserModCommand(int userId, String command, LinkedList<String> segments) {
		this.userId = userId;
		this.command = command;
		this.segments = segments;
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
	public LinkedList<String> getSegments() {
		return segments;
	}
	public void setSegments(LinkedList<String> segments) {
		this.segments = segments;
	}
	
	public void addSegment (String segment){
		this.segments.add(segment);
	}
	
	public String toLine (){
		StringBuilder sb = new StringBuilder();
		sb.append(getUserId()  + " " + getCommand() + " ");
		for (String s: this.segments){
			sb.append(s + " ");
		}
		return sb.toString();
	}
}
