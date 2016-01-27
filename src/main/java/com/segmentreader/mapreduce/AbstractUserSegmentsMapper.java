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
import com.segmentreader.useroperations.OperationHandler;

public abstract class AbstractUserSegmentsMapper extends Mapper<NullWritable, UserModCommand, NullWritable, Text> {
	private static final Logger logger = LoggerFactory.getLogger(AbstractUserSegmentsMapper.class);
	private static final String mapCounter = "mycounter";
	private static final String appName = "segmentreader";
	
	//class dependencies
	private List<Closeable> closeables = getCloseables();
	private Map<String, OperationHandler> handlers = getHandlers();

	public void map(NullWritable key, UserModCommand value, Context context)
			throws IOException, InterruptedException {
	   logger.info("I started MapReduce job!");
	   handlers.get(value.getCommand()).handle(value);

	   NullWritable nw = NullWritable.get();
	   context.getCounter(appName, mapCounter).increment(1);
	   context.write(nw, new Text(value.toLine()));
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (Closeable closeable: closeables) {
			closeable.close();
		}
	}
	
	protected abstract Map<String, OperationHandler> getHandlers();
	protected abstract List<Closeable> getCloseables();
	
}