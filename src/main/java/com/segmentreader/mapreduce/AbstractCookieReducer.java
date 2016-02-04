package com.segmentreader.mapreduce;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;
import com.segmentreader.useroperations.OperationHandler;

public abstract class AbstractCookieReducer extends
        Reducer<Text, UserModCommand, Text, Text> {

    private Map<String, OperationHandler> handlers = getHandlers();
    private static final String reduceCounter = "reduce_counter";
    private static final String appName = "segmentreader";
    private static final String addOp = OperationHandler.ADD_OPERATION;
    private static final String deleteOp = OperationHandler.DELETE_OPERATION;

    @Override
    public void reduce( Text key, Iterable<UserModCommand> values, Context context)
            throws IOException, InterruptedException {

        List<UserModCommand> userModList = Lists.newArrayList(values);
        
        //grouping usermodcommand's list by command types
        Map <String, List<UserModCommand>> commandsMap = userModList.stream()
                .collect(Collectors.groupingBy(UserModCommand::getCommand));
        
        //creating single usermodcommand for every command type
        UserModCommand addCommand = new UserModCommand(key.toString(),addOp,new LinkedList<>());
        Optional<UserModCommand> addCommandOpt = Optional.of(addCommand);
        //getting merged list from every list in every UMC
        if (addCommandOpt.isPresent()){
            addCommand.setSegments(
                    commandsMap.get(addOp).stream()
                    .map(UserModCommand::getSegments).collect(Collectors.toList())
                    .stream().flatMap(List::stream).collect(Collectors.toList()));
            handlers.get(addOp).handle(addCommand);
        }
        
        UserModCommand deleteCommand = new UserModCommand(key.toString(),deleteOp,new LinkedList<>());
        Optional<UserModCommand> deleteCommandOpt = Optional.of(addCommand);
        if (deleteCommandOpt.isPresent()){
            deleteCommand.setSegments(
                    commandsMap.get(deleteOp).stream()
                    .map(UserModCommand::getSegments).collect(Collectors.toList())
                    .stream().flatMap(List::stream).collect(Collectors.toList()));
        }
        //as result calling handle operations only two times
        
        handlers.get(deleteOp).handle(deleteCommand);
    
        context.getCounter(appName,reduceCounter).increment(1);
    }

     protected abstract Map<String, OperationHandler> getHandlers();
}