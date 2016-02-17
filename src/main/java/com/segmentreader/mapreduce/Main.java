package com.segmentreader.mapreduce;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.segmentreader.utils.CSVConverter;
import com.segmentreader.utils.UserModContainer;
import javafx.util.Pair;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import joptsimple.util.KeyValuePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.segmentreader.useroperations.OperationHandler;

public class Main extends Configured implements Tool {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static Map<String, OperationHandler> handlers;

    public static void main(String[] args) throws Exception {
          int res = ToolRunner.run(new Configuration(), new Main(), args);
          logger.info("Application has finished execution with result: " + res);
          System.exit(res);
    }

    public int run(String args[]) throws Exception {
        int ret = 0;
        OptionParser optionParser = new OptionParser("i:o:");
        OptionSpec<String> inputOptionSpec = optionParser.accepts("i", "input path").withRequiredArg().ofType(String.class).required();
        OptionSpec<String> outPutOptionSpec = optionParser.accepts("o", "output path").withRequiredArg().ofType(String.class).required();
        try {
            OptionSet options = optionParser.parse(args);
            String inputFile = options.valueOf(inputOptionSpec);
            String outputPath = options.valueOf(outPutOptionSpec);
            Job job = createJob();
            FileInputFormat.addInputPath(job, new Path(inputFile));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.submit();
            logger.info("Job id: {}", job.getJobID());
            ret = job.waitForCompletion(true) ? 0 : -1;
        } catch (OptionException e) {
            optionParser.printHelpOn(System.out);
            ret = -1;
        }
        return ret;
    }

    private Job createJob() throws IOException {
        Job job = new Job(getConf(), "parquetreader");
        job.setJarByClass(Main.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserModContainer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(AppContext.UserSegmentsMapper.class);
       // job.setCombinerClass(AppContext.CookieReducer.class);
        job.setReducerClass(AppContext.CookieReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        logger.info("Mapreduce job created");
        return job;
    }

}
