package com.segmentreader.mapreduce;

import java.io.IOException;
import java.io.Closeable;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.segmentreader.mapreduce.UserModCommand;
import com.segmentreader.useroperations.OperationHandler;

public abstract class AbstractUserSegmentsMapper extends
        Mapper<NullWritable, UserModCommand, NullWritable, NullWritable> {
    private static final Logger logger = LoggerFactory
            .getLogger(AbstractUserSegmentsMapper.class);
    private static final String mapCounter = "mycounter";
    private static final String errorCounter = "errorcounter";
    private static final String appName = "segmentreader";

    // class dependencies
    private List<Closeable> closeables = getCloseables();
    private Map<String, OperationHandler> handlers = getHandlers();

    public void map(NullWritable key, UserModCommand value, Context context)
            throws IOException, InterruptedException {
        logger.debug("Map job started");
        try {
            handlers.get(value.getCommand()).handle(value);
            //context.write(NullWritable.get(), new Text(value.toLine()));
            context.getCounter(appName, mapCounter).increment(1);
        } catch (IOException e) {
            logger.error("Exception occured. User: {}, exception code: {}", value.getUserId(), e);
            context.getCounter(appName, errorCounter).increment(1);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        for (Closeable closeable : closeables) {
            closeable.close();
        }
        logger.info("Clean up completed");
    }

    protected abstract Map<String, OperationHandler> getHandlers();

    protected abstract List<Closeable> getCloseables();

}