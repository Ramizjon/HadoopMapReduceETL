package logic;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.record.Record;
import org.apache.hadoop.util.Tool;
import org.hsqldb.persist.Log;
import org.junit.experimental.theories.Theories;

import IO.UserModInputFormat;
import IO_CSV.UserModInputFormatTest;

import com.sun.net.ssl.internal.www.protocol.https.Handler;

import dataTO.UserModCommand;
import domain.User;
import experimental.ParquetProcessor;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroup;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.example.ExampleInputFormat;
import parquet.hadoop.example.GroupReadSupport;
import parquet.schema.*;
import parquet.tools.read.SimpleReadSupport;
import parquet.tools.read.SimpleRecord;
import parquet.tools.read.SimpleRecordMaterializer;

public class Main{
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
    
		Job job = new Job(conf, "parquetreader");
		job.setJarByClass(Main.class);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	        
	    job.setMapperClass(ParquetMapper.class);
	    
	    job.setInputFormatClass(UserModInputFormatTest.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	    job.waitForCompletion(true);
	}
}
