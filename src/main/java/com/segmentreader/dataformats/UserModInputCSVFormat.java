package com.segmentreader.dataformats;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import com.segmentreader.mapreduce.UserModCommand;


import java.io.IOException;
import java.util.LinkedList;



public class UserModInputCSVFormat extends FileInputFormat<NullWritable, UserModCommand> {
	
    public RecordReader<NullWritable, UserModCommand> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new UserModInputFormatTestClass();
        
    }

    public class UserModInputFormatTestClass extends RecordReader<NullWritable, UserModCommand> {
        private LineRecordReader lineRecordReader = null;
        private Text key = null;
        private UserModCommand value = null;

        @Override
        public void close() throws IOException {
            if (null != lineRecordReader) {
                lineRecordReader.close();
                lineRecordReader = null;
            }
            key = null;
            value = null;
        }

        @Override
        public NullWritable getCurrentKey() throws IOException, InterruptedException {
            return null;
        }

        @Override
        public UserModCommand getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return lineRecordReader.getProgress();
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            close();

            lineRecordReader = new LineRecordReader();
            lineRecordReader.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!lineRecordReader.nextKeyValue()) {
                key = null;
                value = null;
                return false;
            }

            Text line = lineRecordReader.getCurrentValue();
            String str = line.toString();
            String[] arr = str.split(",");
            
            if (arr.length < 3){
                throw new IOException("Validation failed: not enough arguments");
            }

            LinkedList<String> segmentsList = new LinkedList<String>();
            for (int i = 2; i < arr.length; i++){
            	segmentsList.add(arr[i]);
            }
            key = null;
            value = new UserModCommand(arr[0], arr[1], segmentsList);

            return true;
        }
    }
}