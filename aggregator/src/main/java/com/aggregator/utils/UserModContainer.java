package com.aggregator.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.WritableComparable;

import java.io.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserModContainer<T extends Comparable<T>> implements WritableComparable<UserModContainer>, Serializable {

    private T data;

    @Override
    public int compareTo(UserModContainer o) {
        return this.data.compareTo((T) o.getData());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        byte[] bytes = null;
        try(ByteArrayOutputStream bos
                    = new ByteArrayOutputStream();
            ObjectOutputStream oos
                    = new ObjectOutputStream (bos);
        ) {
            oos.writeObject(this. data);
            oos.flush();
            bytes = bos.toByteArray();
        }
        out.write(bytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        IOUtils.copy((DataInputStream)in, bos);
        byte[] bytes = bos.toByteArray();
        try( ByteArrayInputStream byteInputStream
                     = new ByteArrayInputStream(bytes);
             ObjectInputStream ois
                     = new ObjectInputStream(byteInputStream);
        ){
            data = (T) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString(){
        return data.toString();
    }
}
