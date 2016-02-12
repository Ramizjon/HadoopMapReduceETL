package com.segmentreader.mapreduce;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.io.ByteStreams;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.beans.Transient;
import java.io.*;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class UserModCommand implements Serializable {
    Instant timestamp;
    String userId;
    String command;

     ArrayList<String> segments;


    public UserModCommand(Instant timestamp, String userId, String command, ArrayList<String> segments) {
        this.userId = userId;
        this.command = command;
        this.segments = segments;
        this.timestamp = timestamp;
    }

    public UserModCommand() {
        segments = new ArrayList<>();
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public List<String> getSegments() {
        return segments;
    }

    public void setSegments(ArrayList<String> segments) {
        this.segments = segments;
    }

    public void addSegment(String segment) {
        this.segments.add(segment);
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if ((o == null) || !(o instanceof UserModCommand)) {
            return false;
        }

        UserModCommand temp = (UserModCommand) o;

        if (!temp.getUserId().equals(this.getUserId())
                || !temp.getCommand().equals(this.getCommand())
                || !temp.getSegments().equals(this.getSegments())
                || !temp.getTimestamp().equals(this.getTimestamp())) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int stPoint = 14;
        int result = 1;
        result = result * stPoint + getCommand().hashCode();
        result = result * stPoint + getUserId().hashCode();
        result = result * stPoint + getSegments().hashCode();
        result = result * stPoint + getTimestamp().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "UserModCommand{" +
                "timestamp=" + timestamp +
                ", userId='" + userId + '\'' +
                ", command='" + command + '\'' +
                ", segments=" + segments +
                '}';
    }


    private Object toObject(byte[] bytes) throws IOException, ClassNotFoundException {
        Object obj = null;
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        try {
            bis = new ByteArrayInputStream(bytes);
            ois = new ObjectInputStream(bis);
            obj = ois.readObject();
        } finally {
            if (bis != null) {
                bis.close();
            }
            if (ois != null) {
                ois.close();
            }
        }
        return obj;
    }

}
