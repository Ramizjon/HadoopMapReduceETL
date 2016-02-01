package com.segmentreader.mapreduce;

import java.io.IOException;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main extends Configured implements Tool {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Main(), args);
        logger.info("Application has finished execution with result: " + res);
        System.exit(res);
    }

    public int run(String args[]) throws Exception {
        int ret = 0;
        OptionParser optionParser = new OptionParser("i:");
        optionParser.accepts("verbose");
        OptionSpec<String> inputOptionSpec = optionParser.accepts("i","input path").withRequiredArg().ofType(String.class).required();
        try {
            OptionSet options = optionParser.parse(args);
            if (options.has("verbose")){
                System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
            }
            String inputFile = options.valueOf(inputOptionSpec);
            Job job = createJob();
            FileInputFormat.addInputPath(job, new Path(inputFile));
            job.submit();
            // FileOutputFormat.setOutputPath(job, new Path(outputFile));
            logger.info("Job id: {}", job.getJobID());
            ret =  job.waitForCompletion(true) ? 0 : -1;
        }
        catch (OptionException e) {
            optionParser.printHelpOn(System.out);  
            ret = -1;
        }
        return ret;
    }

    private Job createJob() throws IOException {
        Job job = new Job(getConf(), "parquetreader");
        job.setJarByClass(Main.class);
        job.setNumReduceTasks(0);
        
        //job.setMapOutputKeyClass(NullWritable.class);
        //job.setMapOutputValueClass(NullWritable.class);

        //job.setOutputKeyClass(NullWritable.class);
        //job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(AppContext.UserSegmentsMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        
        job.setOutputFormatClass(NullOutputFormat.class);
        
        logger.info("Mapreduce job created");
        return job;
    }

}
