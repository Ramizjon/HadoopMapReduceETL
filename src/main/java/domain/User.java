package domain;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;

public class User {
	int userId;
	String segment;
	
	public User(int userId, String segment) {
		super();
		this.userId = userId;
		this.segment = segment;
	}
	
	public int getUserId() {
		return userId;
	}
	public void setUserId(int userId) {
		this.userId = userId;
	}

	public String getSegment() {
		return segment;
	}

	public void setSegment(String segment) {
		this.segment = segment;
	}
}
