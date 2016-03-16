package com.aggregator.useroperations;

import com.aggregator.domain.UserRepository;
import com.common.mapreduce.ReducerUserModCommand;

import java.io.IOException;


public abstract class DeleteOperationHandler implements OperationHandler {

    UserRepository instance = getRepoInstance();

    @Override
    public void handle(ReducerUserModCommand value) throws IOException {
        instance.removeUser(value);
    }

    protected abstract UserRepository getRepoInstance();

}
