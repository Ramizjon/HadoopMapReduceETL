package IO;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import dataTO.UserModCommand;
import parquet.example.data.Group;
import parquet.example.data.simple.convert.GroupRecordConverter;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.api.ReadSupport.ReadContext;
import parquet.hadoop.example.ExampleInputFormat;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;
import parquet.tools.read.SimpleRecordConverter;

public class UserModReadSupport extends ReadSupport<UserModCommand> {

	  @Override
	  public parquet.hadoop.api.ReadSupport.ReadContext init(
	      Configuration configuration, Map<String, String> keyValueMetaData,
	      MessageType fileSchema) {
	    String partialSchemaString = configuration.get(ReadSupport.PARQUET_READ_SCHEMA);
	    MessageType requestedProjection = getSchemaForRead(fileSchema, partialSchemaString);
	    return new ReadContext(requestedProjection);
	  }

	@Override
	public RecordMaterializer<UserModCommand> prepareForRead(
			Configuration configuration, Map<String, String> keyValueMetaData,
			MessageType fileSchema,
			parquet.hadoop.api.ReadSupport.ReadContext readContext) {
			return new UserModRecordConverter(readContext.getRequestedSchema());
	}
}
