package com.segmentreader.domain;

import java.io.IOException;

import com.segmentreader.mapreduce.MapperUserModCommand;
import com.segmentreader.mapreduce.ReducerUserModCommand;

public interface UserRepository {
	public void addUser(ReducerUserModCommand user) throws IOException;
	public void removeUser(ReducerUserModCommand user) throws IOException;
}