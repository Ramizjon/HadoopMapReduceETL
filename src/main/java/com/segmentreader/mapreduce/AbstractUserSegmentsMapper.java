package com.segmentreader.mapreduce;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.segmentreader.utils.UserModContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.segmentreader.dataformats.Convertor;

@Slf4j
public abstract class AbstractUserSegmentsMapper extends
        Mapper<LongWritable, Text, Text, UserModContainer<MapperUserModCommand>> {
    
    private static final Logger logger = LoggerFactory
            .getLogger(AbstractUserSegmentsMapper.class);

    private static final String mapCounter = "mapcounter";
    private static final String errorCounter = "map_error_counter";
    private static final String appName = "segmentreader";

    // class dependencies
    private List<Closeable> closeables = getCloseables();
    private Convertor convertor = getConvertor();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        logger.debug("Map job started");
        MapperUserModCommand cmd = null;

        try{
            cmd = convertor.convert(value.toString());
            UserModContainer<MapperUserModCommand> umc = new UserModContainer<>(cmd);
            log.info("MAPPER WILL WRITE: {}", umc.toString());
            context.write(new Text(cmd.getUserId()), umc);
            context.getCounter(appName, mapCounter).increment(1);
        } catch (InvalidArgumentException e) {
            logger.error("Exception occured. Arguments: {}, exception code: {}", value.toString(), e);
            context.getCounter(appName, errorCounter).increment(1);
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

    protected abstract List<Closeable> getCloseables();
    protected abstract Convertor getConvertor();

}