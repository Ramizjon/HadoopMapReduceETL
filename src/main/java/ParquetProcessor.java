import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;

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
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import parquet.schema.Type;


public class ParquetProcessor {
	
	
	public List<Type> parseGroup (Group group){
		List<Type> cells;
		GroupType type = group.getType();
		cells = type.getFields();
		//if (cells.get(1).equals(other))
		return cells;
	}
	
	
	public MessageType readParquetFile (String filePath, Job job){
		  String inputFile = filePath;
		  Path parquetFilePath = null;
		  RemoteIterator<LocatedFileStatus> it;
		  MessageType schema = null;
		  try{
		  it = FileSystem.get(job.getConfiguration()).listFiles(new Path(inputFile), true);
		  while(it.hasNext()) {
		      FileStatus fs = it.next();
		    if(fs.isFile()) {
		      parquetFilePath = fs.getPath();
		      break;
		    }
		  }
		  
		  if(parquetFilePath == null) {
		    System.err.println("No file found for " + inputFile);
		    return null;
		  }
		  ParquetMetadata readFooter = ParquetFileReader.readFooter(job.getConfiguration(), parquetFilePath);
		   schema = readFooter.getFileMetaData().getSchema();
		  List<ColumnDescriptor> columnList = schema.getColumns();
		  //GroupWriteSupport.setSchema(schema, job.getConfiguration());
		  job.submit();
		  }
		  catch (Exception e){
			  e.printStackTrace();
		  }
		  return schema;
	}
}
