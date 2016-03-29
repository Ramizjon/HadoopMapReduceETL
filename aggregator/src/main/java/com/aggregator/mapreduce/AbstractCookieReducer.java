package com.aggregator.mapreduce;

import com.aggregator.useroperations.OperationHandler;
import com.aggregator.utils.UserModContainer;
import com.common.mapreduce.ReducerUserModCommand;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.Closeable;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractCookieReducer extends AbstractReducer <Void, GenericRecord> {

    private Map<String, OperationHandler> handlers = getHandlers();
    private List<Closeable> closeables = getCloseables();

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        for (Closeable closeable : closeables) {
            closeable.close();
        }
        log.debug("Clean up completed");
    }

    protected void writeToContext(Map<String, String> readyMap, String command, Text key, Context context) {
        ReducerUserModCommand rumc = new ReducerUserModCommand(key.toString(), command, readyMap);
        try {
            handlers.get(command).handle(rumc);
            Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/rumcSchema.avsc"));
            GenericRecord record = new GenericData.Record(schema);
            record.put("userid", rumc.getUserId());
            record.put("command", rumc.getCommand());
            record.put("segmenttimestamps", rumc.getSegmentTimestamps());
            context.write(null, record);
            context.getCounter(appName, reduceCounter).increment(1);
        } catch (IOException | InterruptedException e) {
            log.error("Exception occured. Arguments: {}, exception code: {}", key.toString(), e);
            context.getCounter(appName, errorCounter).increment(1);
        }
    }

    protected abstract List<Closeable> getCloseables();

    protected abstract Map<String, OperationHandler> getHandlers();

    @Override
    protected String getReducerType() {
        return "reducer";
    }

}