package com.segmentreader.domain;

import java.io.IOException;

public interface UserRepository {
	public void addUser(User user) throws IOException;
	public void removeUser(String userId) throws IOException;
}