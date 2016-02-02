package com.segmentreader.domain;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

public interface UserRepository {
	public void addUser(Instant timestamp, String userId, List<String> segments) throws IOException;
	public void removeUser(String userId) throws IOException;
}