package com.segmentreader.domain;
import java.util.AbstractMap.SimpleEntry;
import java.util.LinkedList;
import java.util.Map;

public class User {
	int userId;
	LinkedList<String> segments;
	
	public User(int userId, LinkedList<String> segments) {
		super();
		this.userId = userId;
		this.segments = segments;
	}
	
	public int getUserId() {
		return userId;
	}
	public void setUserId(int userId) {
		this.userId = userId;
	}

	public LinkedList<String> getSegments() {
		return segments;
	}

	public void setSegments(LinkedList<String> segment) {
		this.segments = segment;
	}
}
