package com.segmentreader.domain;

import java.util.LinkedList;

public interface UserRepository {
	public void addUser(int userId, LinkedList<String> segments);
	public void removeUser(String rowId, LinkedList<String> segments);
}
