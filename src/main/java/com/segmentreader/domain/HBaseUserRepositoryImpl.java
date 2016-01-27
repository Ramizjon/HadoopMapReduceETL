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

public class HBaseUserRepositoryImpl implements UserRepository, Closeable {
    LinkedList<User> cachedList;
    private final int BUFFER_SIZE = 1;
    private static HBaseUserRepositoryImpl instance;
    private Configuration config;
    private HTable hTable;

    public HBaseUserRepositoryImpl() {
        cachedList = new LinkedList<User>();
        config = HBaseConfiguration.create();
        try {
            hTable = new HTable(config, "users");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void addUser(int userId, LinkedList<String> segments) {
        cachedList.add(new User(userId, segments));
        this.checkForBulk();
    }

    private void checkForBulk() {
        if (cachedList.size() == BUFFER_SIZE) {
            this.flush();
            cachedList.clear();
        }
    }

    protected void flush() {
        Put put = null;
        for (User u : cachedList) {
            put = new Put(Bytes.toBytes("Id" + String.valueOf(u.getUserId())));
            for (String segm : u.getSegments()) {
                String timeStamp = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
                        .format(System.currentTimeMillis());
                put.add(Bytes.toBytes("general"), Bytes.toBytes(segm),
                        Bytes.toBytes(timeStamp));
                try {
                    hTable.put(put);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void removeUser(String rowId, LinkedList<String> segments) {
        Delete delete = new Delete(Bytes.toBytes(rowId));
        for (String s : segments) {
            delete.deleteColumns(Bytes.toBytes("general"), Bytes.toBytes(s));
        }
        try {
            hTable.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
    }

}
