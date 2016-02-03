package com.segmentreader.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.Map;
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
       
       
        /*
        Iterator<UserModCommand> iterator = values.iterator();
        Map <String, UserModCommand> commandsMap = new HashMap<>();
        commandsMap.put(addOp, new UserModCommand(key.toString(), addOp, new LinkedList<>()));
        commandsMap.put(deleteOp, new UserModCommand(key.toString(), deleteOp, new LinkedList<>()));
        
        while(iterator.hasNext()) { 
            UserModCommand umc = iterator.next();
            commandsMap.get(umc.getCommand()).addSegments(umc.getSegments());
        }
        */
        
        
        //Java 8 solution
        List<UserModCommand> userModList = Lists.newArrayList(values);
        
        //grouping usermodcommand's list by command types
        Map <String, List<UserModCommand>> commandsMap = userModList.stream()
                .collect(Collectors.groupingBy(UserModCommand::getCommand));
        
        //creating single usermodcommand for every command type
        UserModCommand addCommand = new UserModCommand();
        //getting merged list from every list in every UMC
        addCommand.setSegments(
                commandsMap.get(addOp).stream()
                .map(UserModCommand::getSegments).collect(Collectors.toList())
                .stream().flatMap(List::stream).collect(Collectors.toList()));
        
        UserModCommand deleteCommand = new UserModCommand();
        deleteCommand.setSegments(
                commandsMap.get(deleteOp).stream()
                .map(UserModCommand::getSegments).collect(Collectors.toList())
                .stream().flatMap(List::stream).collect(Collectors.toList()));

        //as result calling handle operations only two times
        handlers.get(addOp).handle(addCommand);
        handlers.get(deleteOp).handle(deleteCommand);
    
        context.getCounter(appName,reduceCounter).increment(1);
        context.write(new Text("hi"), new Text("there"));
    }

     protected abstract Map<String, OperationHandler> getHandlers();
}