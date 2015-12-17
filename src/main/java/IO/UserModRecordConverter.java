package IO;

import dataTO.UserModCommand;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

public class UserModRecordConverter extends RecordMaterializer<UserModCommand> {

	  public UserModRecordConverter(MessageType schema) {
	    this.root = new UserModRecordConverter() {
	      @Override
	      public void start() {
	        this.current = new UserModCommand();
	      }

	      @Override
	      public void end() {
	      }
	    };
	  }

	  @Override
	  public Group getCurrentRecord() {
	    return root.getCurrentRecord();
	  }

	  @Override
	  public GroupConverter getRootConverter() {
	    return root;
	  }

	}
