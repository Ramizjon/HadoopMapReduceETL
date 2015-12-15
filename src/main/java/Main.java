import java.io.IOException;
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
import org.apache.hadoop.util.Tool;

import parquet.example.data.Group;
import parquet.hadoop.example.ExampleInputFormat;
import parquet.schema.*;


public class Main extends Configured implements Tool{
	public static class ParquetMapper extends Mapper<LongWritable, Group, Text, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();

	    public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
	        ParquetProcessor parquetProcessor = new ParquetProcessor();
	    	List<Type> values = parquetProcessor.parseGroup(value);
	        StringTokenizer tokenizer = new StringTokenizer(values.toString());
	        while (tokenizer.hasMoreTokens()) {
	            word.set(tokenizer.nextToken());
	            context.write(word, one);
	        }
	    }
	 }
	
	public static void main(String[] args) throws Exception {

	 }

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());

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
