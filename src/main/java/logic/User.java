package logic;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;

public class User {
	int userId;
	SimpleEntry <Boolean, String> operationKeyValuePair;
	
	public User(int userId, SimpleEntry<Boolean, String> operationKeyValuePair) {
		super();
		this.userId = userId;
		this.operationKeyValuePair = operationKeyValuePair;
	}
	
	public int getUserId() {
		return userId;
	}
	public void setUserId(int userId) {
		this.userId = userId;
	}

	public SimpleEntry<Boolean, String> getOperationKeyValuePair() {
		return operationKeyValuePair;
	}

	public void setOperationKeyValuePair(
			SimpleEntry<Boolean, String> operationKeyValuePair) {
		this.operationKeyValuePair = operationKeyValuePair;
	}

	@Override
	public String toString() {
		return "User [userId=" + userId + ", operationKeyValuePair="
				+ operationKeyValuePair + "]";
	}

	
}
