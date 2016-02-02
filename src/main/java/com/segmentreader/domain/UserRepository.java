package com.segmentreader.domain;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

public interface UserRepository {
	public void addUser(Timestamp timestamp, String userId, List<String> segments) throws IOException;
	public void removeUser(String userId) throws IOException;
}