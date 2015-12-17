package experimental;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

import domain.User;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroup;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.example.ExampleInputFormat;
import parquet.hadoop.example.GroupReadSupport;
import parquet.schema.*;
import parquet.tools.read.SimpleReadSupport;
import parquet.tools.read.SimpleRecord;
import parquet.tools.read.SimpleRecordMaterializer;

public class Experimental{
	public static class ParquetMapper extends Mapper<LongWritable, Group, Text, IntWritable> {
	    public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
	    	
	    }
	 }
	
	//public static void main(String[] args) throws Exception {
		/*ParquetProcessor proc = new ParquetProcessor();
		PrintWriter writer = new PrintWriter(System.out, true);
		LinkedList <User> usersList = proc.readParquetFile(args[0]);
		for (User user: usersList){
			writer.append(user.toString());
			writer.println();
		}*/
	//}

	public int run(String[] args) throws Exception {
		Job job = new Job();

	    job.setJarByClass(getClass());
	    job.setJobName(getClass().getName());
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapperClass(ParquetMapper.class);
	    job.setNumReduceTasks(0);

	    job.setInputFormatClass(ExampleInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	       
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	       
	    job.waitForCompletion(true);
		return 0;
	}
}
