package IO_CSV;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContext;

import dataTO.UserModCommand;

public class CustomCsvInputFormat extends FileInputFormat<LongWritable,Text>{
	
	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
		return (RecordReader<LongWritable, Text>) new CustomLineRecordReader();
	}
	
}