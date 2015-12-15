import java.util.Map;

public class User {
	String userId;
	Map <String,String> operationsMap;
	
	public User(String userId, Map<String, String> operationsMap) {
		super();
		this.userId = userId;
		this.operationsMap = operationsMap;
	}
	
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public Map<String, String> getOperationsMap() {
		return operationsMap;
	}
	public void setOperationsMap(Map<String, String> operationsMap) {
		this.operationsMap = operationsMap;
	}
	
}
