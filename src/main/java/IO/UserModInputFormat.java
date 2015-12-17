package IO;

import parquet.example.data.Group;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.example.GroupReadSupport;

public class UserModInputFormat extends ParquetInputFormat<Group> {

	  public UserModInputFormat() {
	    super(UserModReadSupport.class);
	  }

}