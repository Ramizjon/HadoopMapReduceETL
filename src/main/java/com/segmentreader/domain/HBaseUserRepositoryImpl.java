package com.segmentreader.domain;

import java.io.Closeable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.segmentreader.mapreduce.AbstractUserSegmentsMapper;

public class HBaseUserRepositoryImpl implements UserRepository, Closeable {
    private static final Logger logger = LoggerFactory
            .getLogger(HBaseUserRepositoryImpl.class);

    private static final int BUFFER_SIZE = 1;
    private static final String COLUMN_FAMILY = "general";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
            "yyyy/MM/dd HH:mm:ss");

    LinkedList<User> cachedList;
    private HTable hTable;

    public HBaseUserRepositoryImpl() throws IOException {
        cachedList = new LinkedList<User>();
        Configuration config = HBaseConfiguration.create();
        hTable = new HTable(config, "users");
    }

    @Override
    public void addUser(String userId, LinkedList<String> segments)
            throws IOException {
        cachedList.add(new User(userId, segments));
        this.checkForBulk();
    }

    private void checkForBulk() throws IOException {
        if (cachedList.size() == BUFFER_SIZE) {
            this.flush();
            cachedList.clear();
        }
    }

    protected void flush() throws IOException {
        Put put = null;
        for (User u : cachedList) {
            put = new Put(Bytes.toBytes(u.getUserId()));
            for (String segm : u.getSegments()) {
                String timeStamp = DATE_FORMAT.format(System.currentTimeMillis());
                put.add(Bytes.toBytes(COLUMN_FAMILY),
                        Bytes.toBytes(segm),
                        Bytes.toBytes(timeStamp));
            }
            hTable.put(put);
        }
    }

    @Override
    public void removeUser(String rowId)
            throws IOException {
        Delete delete = new Delete(Bytes.toBytes(rowId));
        hTable.delete(delete);
        logger.debug("User removed from row {}", rowId);
    }

    @Override
    public void close() throws IOException {
        flush();
    }

}
