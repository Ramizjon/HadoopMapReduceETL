package IO;

import dataTO.UserModCommand;
import parquet.example.data.Group;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.example.ExampleInputFormat;
import parquet.hadoop.example.GroupReadSupport;

public class UserModInputFormat extends ParquetInputFormat<UserModCommand> {

	  public UserModInputFormat() {
	    super(UserModReadSupport.class);
	  }

}