package com.segmentreader.utils;

import com.segmentreader.mapreduce.UserModCommand;
import org.apache.hadoop.io.WritableComparable;

import java.io.*;

public class UserModContainer implements WritableComparable, Serializable {

    private UserModCommand userModCommand;

    public UserModCommand get() {
        return userModCommand;
    }

    public void put(UserModCommand umc) {
        userModCommand = umc;
    }

    public UserModContainer(UserModCommand userModCommand) {
        this.userModCommand = userModCommand;
    }

    @Override
    public int compareTo(Object o) {
        if (!(o instanceof UserModCommand)) {
            throw new IllegalArgumentException();
        }
        UserModCommand inputUmc = (UserModCommand) o;
        int result = 0;
        result = userModCommand.getTimestamp().compareTo(((UserModCommand) o).getTimestamp());
        if (result == 0){
            if (userModCommand.getUserId().equals(((UserModCommand) o).getUserId())
                    && userModCommand.getCommand().equals(((UserModCommand) o).getCommand())) {
                result = 0;
            }
        }
        return result;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        byte[] bytes = null;
        try(ByteArrayOutputStream bos
                    = new ByteArrayOutputStream();
            ObjectOutputStream oos
                    = new ObjectOutputStream (bos);
        ) {
            oos.writeObject(this.userModCommand);
            oos.flush();
            bytes = bos.toByteArray();
        }
        out.write(bytes);
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        UserModCommand umc = null;
        try( ByteArrayInputStream byteInputStream
                     =  (ByteArrayInputStream)in;
             ObjectInputStream ois
                     = new ObjectInputStream(byteInputStream);
        ){
            umc = (UserModCommand) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        this.put(umc);
    }
}
