package com.receiver.mapreduce;

import com.common.mapreduce.MapperUserModCommand;
import com.receiver.facebookprovider.FacebookProvider;
import com.receiver.nexusprovider.NexusProvider;
import com.receiver.utils.Provider;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Triple;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class Main extends Configured implements Tool {


    private final List<Triple<String,String,Provider>> tripleProvidersList = Arrays.asList(
        new Triple<String, String, Provider>("n", "nexus data provider input path", new NexusProvider()),
        new Triple<String,String, Provider>("f", "facebook data provider input path", new FacebookProvider())
    );

    public static void main(String[] args) throws Exception {
          int res = ToolRunner.run(new Configuration(), new Main(), args);
          log.info("Application has finished execution with result: " + res);
          System.exit(res);
    }

    public int run(String args[]) throws Exception {
        int ret = 0;
        OptionParser optionParser = new OptionParser("n:f:o:");
        OptionSpec<String> outPutOptionSpec = optionParser.accepts("o", "output path").withRequiredArg().ofType(String.class).required();
        try {
            tripleProvidersList.forEach(e -> {
                OptionSpec<String> optionSpec;
                if (e.getThird().isOptional()){
                    optionSpec = optionParser.accepts(e.getFirst(),
                            e.getSecond()).withOptionalArg().ofType(String.class);
                }
                else {
                    optionSpec = optionParser.accepts(e.getFirst(),
                            e.getSecond()).withRequiredArg().ofType(String.class).required();
                }
                try {
                    OptionSet options = optionParser.parse(args);
                    if (options.hasArgument(optionSpec)) {
                        Job job = createJob(options.valueOf(optionSpec), options.valueOf(outPutOptionSpec), e.getThird().getMapper());
                        job.submit();
                    }
                } catch (IOException | InterruptedException | ClassNotFoundException e1) {
                    throw new RuntimeException(e1);
                }
            });

        } catch (OptionException e) {
            optionParser.printHelpOn(System.out);
            ret = -1;
        }
        return ret;
    }

    private <M extends Mapper<?, ?, Void, GenericRecord>> Job createJob(String inputPath, String outputPath, M type) throws IOException {
        log.info("Info for current createJob call: \ninputpath: {}\noutputPath: {}", inputPath, outputPath);
        Job job = new Job(getConf(), type.getClass().getName());
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath+"/"+type.toString()));

        job.setNumReduceTasks(0);
        job.setJarByClass(Main.class);

        job.setMapOutputKeyClass(Void.class);
        job.setMapOutputValueClass(MapperUserModCommand.class);

        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(MapperUserModCommand.class);
        job.setMapperClass(type.getClass());
        job.setInputFormatClass(TextInputFormat.class);

        Schema schema = ReflectData.get().getSchema(MapperUserModCommand.class);
        AvroParquetOutputFormat.<MapperUserModCommand>setSchema(job, schema);

        job.setOutputFormatClass(AvroParquetOutputFormat.class);

        log.info("Mapreduce job created wit id: {}", job.getJobID());
        return job;
    }


}
