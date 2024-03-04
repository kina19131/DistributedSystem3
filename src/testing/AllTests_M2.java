package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests_M2 {

	static {
		try {
			new LogSetup("logs/testing/M2test.log", Level.ERROR);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(M2BasicTest.class);  
		clientSuite.addTestSuite(M2AdditionalTest.class);
		clientSuite.addTestSuite(M2Test1.class);
		clientSuite.addTestSuite(M2Test2.class);  // NO
		clientSuite.addTestSuite(M2Test3.class); 
		clientSuite.addTestSuite(M2Test4.class);
		clientSuite.addTestSuite(M2Test5.class);
		clientSuite.addTestSuite(M2Test6.class); 
		clientSuite.addTestSuite(M2Test7.class); // iffy
		clientSuite.addTestSuite(M2Test8.class);
		return clientSuite;
	}
	
}
