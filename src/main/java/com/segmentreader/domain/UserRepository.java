package com.segmentreader.domain;

import java.io.IOException;
import java.util.LinkedList;

public interface UserRepository {
	public void addUser(String userId, LinkedList<String> segments) throws IOException;
	public void removeUser(String userId) throws IOException;
}
