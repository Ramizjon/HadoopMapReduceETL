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

public class UserRepositoryImpl implements UserRepository, Closeable {
    LinkedList<User> cachedList;
    private final int BUFFER_SIZE = 1;
    private static UserRepositoryImpl instance;
    private Configuration config;
    private HTable hTable;

    public UserRepositoryImpl() {
        cachedList = new LinkedList<User>();
        config = HBaseConfiguration.create();
        try {
            hTable = new HTable(config, "users");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addUser(int userId, LinkedList<String> segments) {
        cachedList.add(new User(userId, segments));
        this.checkForBulk();
    }

    private void checkForBulk() {
        if (cachedList.size() == BUFFER_SIZE) {
            this.addUsersToHbase();
            cachedList.clear();
        }
    }

    // just for debug
    public String print() {
        StringBuilder sb = new StringBuilder();
        for (User user : cachedList) {
            sb.append(user.getSegments() + " ");
        }
        return ("------------\n" + sb.toString());
    }

    public void addUsersToHbase() {
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

    protected void flush() {
        addUsersToHbase();
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    // TODO implement receiving User from HBase by id
    public User getUser(int userId) {
        return new User(userId, null);
    }

}
