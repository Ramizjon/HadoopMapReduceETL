package IO;

import dataTO.UserModCommand;
import parquet.schema.MessageType;

public class UserModCommandFactory {

	private final MessageType schema;
	
	public UserModCommandFactory (MessageType schema){
		this.schema = schema;
	}
	
	public UserModCommand newUserModCommand(){
		return new UserModCommand();
	}
	
}
