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
	
	public void addSegments (List<String> segments){
	    this.segments.addAll(segments);
	}
	
	@Override
	public boolean equals (Object o){
	    if (this == o)
	        return true;
	    
	    if ((o == null) || !(o instanceof UserModCommand)){
	        return false;
	    }
	    
	    UserModCommand temp = (UserModCommand) o;
	    
	    if(!temp.getUserId().equals(this.getUserId()) ||
	            !temp.getCommand().equals(this.getCommand())||
	            !temp.getSegments().equals(this.getSegments())){
	        return false;
	    }
	    return true;
	}
	
	
	@Override
	public int hashCode(){
	    final int stPoint = 14;
	    int result = 1;
	    result = result * stPoint + getCommand().hashCode();
	    result = result * stPoint + getUserId().hashCode();
	    result = result * stPoint + getSegments().hashCode();
	    return result;
	}
}
