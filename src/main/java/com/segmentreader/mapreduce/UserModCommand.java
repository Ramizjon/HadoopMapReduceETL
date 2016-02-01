package com.segmentreader.mapreduce;

import java.util.LinkedList;
import java.util.List;

public class UserModCommand {
	String userId;
	String command;
	List<String> segments;
	
	public UserModCommand(String userId, String command, List<String> segments) {
		this.userId = userId;
		this.command = command;
		this.segments = segments;
	}
	
	public UserModCommand(){
	    segments = new LinkedList<>();
	}
	
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getCommand() {
		return command;
	}
	public void setCommand(String command) {
		this.command = command;
	}
	public List<String> getSegments() {
		return segments;
	}
	public void setSegments(List<String> segments) {
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
