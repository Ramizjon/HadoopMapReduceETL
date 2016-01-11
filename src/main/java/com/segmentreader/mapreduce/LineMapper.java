package com.segmentreader.mapreduce;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.segmentreader.domain.UserRepository;
import com.segmentreader.useroperations.HandlersFactory;
import com.segmentreader.useroperations.OperationHandler;
import com.sun.java_cup.internal.runtime.virtual_parse_stack;

public  class LineMapper extends Mapper<NullWritable, UserModCommand, NullWritable, Text> {
	private static final Logger logger = LoggerFactory.getLogger(LineMapper.class);
	Map<String, OperationHandler> handlers = new HandlersFactory().getHandlers();
	
	
    public void map(NullWritable key, UserModCommand value, Context context) throws IOException, InterruptedException {
    	logger.info("I started MapReduce job!");
    	handlers.get(value.getCommand()).handle(value);
    	//NullWritable nw = NullWritable.get();
    	//context.write(nw, new Text(UserRepository.getInstance().print()));
    }
    
    
 }