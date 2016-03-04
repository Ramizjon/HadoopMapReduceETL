package com.aggregator.domain;

import java.io.IOException;

import com.aggregator.mapreduce.ReducerUserModCommand;

public interface UserRepository {
	public void addUser(ReducerUserModCommand user) throws IOException;
	public void removeUser(ReducerUserModCommand user) throws IOException;
}