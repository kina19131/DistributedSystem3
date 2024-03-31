package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import app_kvECS.ECSClient;
import app_kvServer.KVServer;

public class AllTests_M3 {

	static {
		try {
			new LogSetup("logs/testing/M3test.log", Level.ERROR);
			final int CACHE_SIZE = 10;
    		final String CACHE_POLICY = "FIFO";

			final ECSClient ecsClient = new ECSClient(51000);
            new Thread(new Runnable() {
                @Override
                public void run() {
                    ecsClient.startListening();
                }
            }).start();
            ecsClient.monitorHeartbeats();

            Thread.sleep(500);

            final KVServer kvServer1 = new KVServer(50002, CACHE_SIZE, CACHE_POLICY, "Node_1");
            new Thread(new Runnable() {
                @Override
                public void run() {
                    kvServer1.run();
                }
            }).start();

            Thread.sleep(500);

            final KVServer kvServer2 = new KVServer(50003, CACHE_SIZE, CACHE_POLICY, "Node_2");
            new Thread(new Runnable() {
                @Override
                public void run() {
                    kvServer1.run();
                }
            }).start();

            Thread.sleep(500);

            final KVServer kvServer3 = new KVServer(50004, CACHE_SIZE, CACHE_POLICY, "Node_3");
            new Thread(new Runnable() {
                @Override
                public void run() {
                    kvServer1.run();
                }
            }).start();

            Thread.sleep(500);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(M3AdditionalTest.class);
		clientSuite.addTestSuite(M3Tests.class);
		clientSuite.addTestSuite(M3Tests2.class);
		return clientSuite;
	}
	
}
