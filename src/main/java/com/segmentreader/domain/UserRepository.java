package com.segmentreader.domain;

import java.io.IOException;

import com.segmentreader.mapreduce.UserModCommand;

public interface UserRepository {
	public void addUser(UserModCommand user) throws IOException;
	public void removeUser(String userId) throws IOException;
}