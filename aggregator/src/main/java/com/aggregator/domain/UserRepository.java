package com.aggregator.domain;

import com.common.mapreduce.ReducerUserModCommand;

import java.io.IOException;

public interface UserRepository {
	public void addUser(ReducerUserModCommand user) throws IOException;
	public void removeUser(ReducerUserModCommand user) throws IOException;
}