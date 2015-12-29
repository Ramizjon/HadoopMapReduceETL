package logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utilities {
	public final Logger logger = LoggerFactory.getLogger( "AnyUniqueStringHere" );
	private static Utilities instance;
	
	public static Utilities getInstance() {
		if (instance == null){
			instance = new Utilities();
		}
		return instance;
	}
}
