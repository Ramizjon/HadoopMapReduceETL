package com.segmentreader.mapreduce;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.segmentreader.dataformats.Convertor;
import com.segmentreader.useroperations.OperationHandler;

public abstract class AbstractUserSegmentsMapper extends
        Mapper<LongWritable, Text, NullWritable, NullWritable> {
    
    private static final Logger logger = LoggerFactory
            .getLogger(AbstractUserSegmentsMapper.class);
    
    private static final String mapCounter = "mycounter";
    private static final String errorCounter = "errorcounter";
    private static final String appName = "segmentreader";

    // class dependencies
    private List<Closeable> closeables = getCloseables();
    private Map<String, OperationHandler> handlers = getHandlers();
    private Convertor convertor = getConvertor();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        logger.debug("Map job started");
        UserModCommand cmd = null;
        try{
            cmd = convertor.convert(value.toString());
            //logger.debug(cmd.toLine());
            handlers.get(cmd.getCommand()).handle(cmd);
            //context.write(NullWritable.get(), new Text(value.toLine()));
            context.getCounter(appName, mapCounter).increment(1);
        } catch (IOException e) {
            logger.error("Exception occured. User: {}, exception code: {}", cmd.getUserId(), e);
            context.getCounter(appName, errorCounter).increment(1);
            throw e;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        for (Closeable closeable : closeables) {
            closeable.close();
        }
        logger.debug("Clean up completed");
    }

    protected abstract Map<String, OperationHandler> getHandlers();
    protected abstract List<Closeable> getCloseables();
    protected abstract Convertor getConvertor();

}