package com.segmentreader.domain;

import java.util.LinkedList;

public interface UserRepository {
	public void addUserToTempQueue(int userId, LinkedList<String> segments);
	public void addUsersToHbase ();
	public void removeUserFromHbase(String rowId, LinkedList<String> segments);
}
