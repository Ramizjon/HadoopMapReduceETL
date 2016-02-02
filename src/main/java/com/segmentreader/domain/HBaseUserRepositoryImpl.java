package com.segmentreader.domain;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;

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

    private static final int BUFFER_SIZE = 20;
    private static final String COLUMN_FAMILY = "general";

    List<User> cachedList;
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
    public void addUser(Instant timestamp,String userId, List<String> segments)
            throws IOException {
        cachedList.add(new User(timestamp, userId, segments));
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
        Put put = null;
        for (User u : cachedList) {
            put = new Put(Bytes.toBytes(u.getUserId()));
            for (String segm : u.getSegments()) {
                String timeStamp = u.getTimestamp().toString();
                put.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(segm),
                        Bytes.toBytes(timeStamp));
            }
            hTable.put(put);
        }
    }

    @Override
    public void removeUser(String rowId) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(rowId));
        hTable.delete(delete);
        logger.debug("User removed from row {}", rowId);
    }

    @Override
    public void close() throws IOException {
        flush();
    }

}
