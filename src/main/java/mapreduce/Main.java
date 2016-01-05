package mapreduce;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import dataformats.UserModInputFormatTest;

public class Main extends Configured implements Tool{
	private  Job job;
	private  Configuration conf;
	
	 public static void main(String args[]) throws Exception {
		    int result = ToolRunner.run(new Main(), args);
		    System.exit(result);
	}

	public int run (String args[]) throws Exception {
		conf = new Configuration();
		createJob();
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	private  void createJob() {
		try {
			job = new Job(conf, "parquetreader");
		} catch (IOException e) {
			e.printStackTrace();
		}
		job.setJarByClass(Main.class);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	        
	    job.setMapperClass(ParquetMapper.class);
	    
	    job.setInputFormatClass(UserModInputFormatTest.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	 
	    //job.waitForCompletion(true);
	}
}
