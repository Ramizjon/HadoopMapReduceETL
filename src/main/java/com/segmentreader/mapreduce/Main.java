package com.segmentreader.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.segmentreader.dataformats.UserModInputCSVFormat;

public class Main extends Configured implements Tool {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Main(), args);
        logger.info("Application has finished execution with result: " + res);
        System.exit(res);
    }

    public int run(String args[]) throws Exception {
        String inputFile = args[0];
        String outputFile = args[1];
        Job job = createJob();
        logger.info("Job id: " + job.getJobID().toString());
        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));
        return job.waitForCompletion(true) ? 0 : -1;
    }

    private Job createJob() throws IOException {
        Job job = new Job(getConf(), "parquetreader");
        job.setJarByClass(Main.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(AppContext.UserSegmentsMapper.class);

        job.setInputFormatClass(UserModInputCSVFormat.class);
        // job.setOutputFormatClass(TextOutputFormat.class);
        logger.info("Mapreduce job created");
        return job;
    }

}
