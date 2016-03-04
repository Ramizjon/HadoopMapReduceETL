package com.aggregator.domain;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import com.aggregator.mapreduce.ReducerUserModCommand;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseUserRepositoryImpl implements UserRepository, Closeable {
    private static final Logger logger = LoggerFactory
            .getLogger(HBaseUserRepositoryImpl.class);

    private static final int BUFFER_SIZE = 1;
    private static final String COLUMN_FAMILY = "general";

    List<ReducerUserModCommand> cachedList;
    private HTable hTable;
    private int bufferSize = BUFFER_SIZE;

    public HBaseUserRepositoryImpl() throws IOException {
        cachedList = new LinkedList<>();
        hTable = createHTable();
    }

    protected HTable createHTable() throws IOException {
        Configuration config = HBaseConfiguration.create();
        return new HTable(config, "users");
    }
    
    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    @Override
    public void addUser(ReducerUserModCommand user)
            throws IOException {
        cachedList.add(user);
        this.checkForBulk();
    }

    public void setHTable(HTable hTableArg) {
        hTable = hTableArg;
    }

    private void checkForBulk() throws IOException {
        if (cachedList.size() == bufferSize) {
            this.flush();
            cachedList.clear();
        }
    }

    protected void flush() throws IOException {
        for (ReducerUserModCommand u : cachedList) {
            Put put = new Put(Bytes.toBytes(u.getUserId()));
            u.getSegmentTimestamps().forEach((a,s) -> {
                put.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(a),
                        Bytes.toBytes(s));
            });
            hTable.put(put);
        }
    }

    @Override
    public void removeUser(ReducerUserModCommand user) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(user.getUserId()));
        user.getSegmentTimestamps().forEach((u,k) -> {
            delete.deleteColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(u));
            try {
                hTable.delete(delete);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            logger.debug("Removed segment <{}> from row <{}>", u, user.getUserId());
        });
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}
