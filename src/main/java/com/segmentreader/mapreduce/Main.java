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

import com.segmentreader.dataformats.UserModInputCSVFormat;

public class Main extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(res);
    }

    public int run(String args[]) throws Exception {
        String inputFile = args[0];
        String outputFile = args[1];
        Job job = createJob();
        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));
        job.waitForCompletion(true);
        return 0;
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

        return job;
    }

}
