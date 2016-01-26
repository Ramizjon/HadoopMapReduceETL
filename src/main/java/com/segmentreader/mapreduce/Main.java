package com.segmentreader.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import com.segmentreader.dataformats.*;

//extends Configured implements Tool
public class Main {
	private Job job;
	private Configuration conf;
	
	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "parquetreader");
		job.setJarByClass(Main.class);
		job.setNumReduceTasks(0);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(LineMapper.class);

		job.setInputFormatClass(UserModInputCSVFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//job.getCounters().findCounter(LineMapper.getCounterName()).getValue();
		
		if (!job.waitForCompletion(true)) {
			throw new IOException("Application hasn't finished correctly");
		}
		
	}

	
	/*
	
	 public static void main(String[] args) throws Exception {
	        try {
	            int res = ToolRunner.run(new Configuration(), new Main(), args);
	            System.exit(res);
	        } catch (Exception e) {
	            e.printStackTrace();
	            System.exit(255);
	        }
	 }
	
	
	  public int run (String args[]) throws Exception { 
		String inputFile = args[0];
		String outputFile = args[1];
		String compression = (args.length > 2) ? args[2] : "none";
		createJob();
		CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
		if(compression.equalsIgnoreCase("snappy")) {
		    codec = CompressionCodecName.SNAPPY;
		} else if(compression.equalsIgnoreCase("gzip")) {
		    codec = CompressionCodecName.GZIP;
		}
		ExampleOutputFormat.setCompression(job, codec);
		
		FileInputFormat.setInputPaths(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));
        job.waitForCompletion(true);
		return 0;   
	  }
	  
	  
	  private void createJob() { 
		try {
			job = new Job(getConf(), "parquetreader");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		job.setJarByClass(Main.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(ArrayWritable.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(ArrayWritable.class);

		job.setMapperClass(LineMapper.class);

		job.setInputFormatClass(UserModInputCSVFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	  }
	  */
	 
}
