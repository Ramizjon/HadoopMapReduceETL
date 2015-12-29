package mapreduce;
import java.io.IOException;

import logging.Utilities;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public  class ParquetMapper extends Mapper<NullWritable, UserModCommand, NullWritable, Text> {
	private final static IntWritable one = new IntWritable(1); 
	
    public void map(NullWritable key, UserModCommand value, Context context) throws IOException, InterruptedException {
    	Utilities.getInstance().logger.info("I started MapReduce job!");
    	OperationHandler handler = new OperationHandler();
    	handler.performOperationByType(value);
    	//NullWritable nw = NullWritable.get();
    	//context.write(nw, new Text(UserRepository.getInstance().print()));
    }
 }