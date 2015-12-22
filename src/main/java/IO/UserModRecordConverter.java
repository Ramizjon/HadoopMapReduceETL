package IO;
import dataTO.UserModCommand;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.example.data.simple.convert.GroupRecordConverter;
import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;
import parquet.tools.read.SimpleRecordConverter;

public class UserModRecordConverter extends RecordMaterializer<UserModCommand> {

	private final UserModCommandFactory userModCommandFactory;
	private UserModConverter root;
	
	public UserModRecordConverter (MessageType schema){
		this.userModCommandFactory = new UserModCommandFactory(schema);
		this.root = new UserModConverter(null, 0, schema) {
		      @Override
		      public void start() {
		        this.current = userModCommandFactory.newUserModCommand();
		      }

		      @Override
		      public void end() {
		      }
		    };
	}
	
	@Override
	public UserModCommand getCurrentRecord() {
		return root.getCurrentRecord();
	}

	@Override
	public GroupConverter getRootConverter() {
		return root;
	}	  
}
