package com.segmentreader.mapreduce;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.segmentreader.domain.UserRepository;
import com.segmentreader.useroperations.HandlersFactory;
import com.segmentreader.useroperations.OperationHandler;
import com.sun.java_cup.internal.runtime.virtual_parse_stack;

public class LineMapper extends Mapper<NullWritable, UserModCommand, NullWritable, Text> {
	private static final Logger logger = LoggerFactory.getLogger(LineMapper.class);
	private static final String mapCounter = "mycounter";
	private static final String appName = "segmentreader";

	Map<String, OperationHandler> handlers = new HandlersFactory().getHandlers();
	//private GenericRecord myGenericRecord = new GenericData.Record(mySchema);
	
	public void map(NullWritable key, UserModCommand value, Context context)
			throws IOException, InterruptedException {
	   logger.info("I started MapReduce job!");
	   handlers.get(value.getCommand()).handle(value);
	
//	   String[] parts = value.toLine().split(" ");
//	   Writable[] data = new Writable[parts.length];
//	   for(int i =0; i<parts.length; i++) {
//	    data[i] =  new BinaryWritable (Binary.fromString(parts[i]));
//	   }
//	   ArrayWritable aw = new ArrayWritable(Writable.class, data);
//	   context.write(nw, aw);
	   NullWritable nw = NullWritable.get();
	   context.getCounter(appName, mapCounter).increment(1);
	   context.write(nw, new Text(value.toLine()));
	}

	
}