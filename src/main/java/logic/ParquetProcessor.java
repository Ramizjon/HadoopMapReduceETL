package logic;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;

import java.io.File;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;

import org.apache.avro.*;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Job;
import org.codehaus.jackson.node.NullNode;
import org.junit.Test;

import parquet.column.ColumnDescriptor;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import parquet.schema.Type;

public class ParquetProcessor {

	public User parseGroup (Group group){
		int userId = group.getInteger(0, 0);
		Boolean operation = group.getGroup(1, 0).getGroup(0, 0).getBoolean(0, 0);
	    String segment = group.getGroup(1, 0).getGroup(0, 0).getString(1, 0);
    	return new User(userId, new SimpleEntry<Boolean, String>(operation, segment));
	}
	
	public LinkedList<User> readParquetFile (String filePath) throws Exception{
		ParquetReader<Group> reader = null;
		LinkedList<User> usersList = new LinkedList<User>();
		try {
		    reader = new ParquetReader<Group>(new Path(filePath), new GroupReadSupport());
		    for (Group group = reader.read(); group != null; group = reader.read()){
		    	usersList.add(this.parseGroup(group));
		    }
		  } finally {
		    if (reader != null) {
		      try {
		        reader.close();
		      } catch (Exception ex) {
		      }
		    }
		  }
		return usersList;
	}
}
