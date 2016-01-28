package com.segmentreader.domain;
import java.util.LinkedList;
import java.util.List;
public class User {
	String userId;
	List<String> segments;
	
	public User(String userId, List<String> segments) {
		super();
		this.userId = userId;
		this.segments = segments;
	}
	
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}

	public List<String> getSegments() {
		return segments;
	}

	public void setSegments(LinkedList<String> segment) {
		this.segments = segment;
	}
}
