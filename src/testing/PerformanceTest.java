package testing;

import org.junit.Test;

import app_kvServer.KVServer;
import client.KVStore;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.SimpleKVMessage;

import static java.lang.Thread.sleep;

import java.io.File;

public class PerformanceTest extends TestCase {

	private KVStore kvClient;
    private KVServer kvServer;
	
    private int NUM_OPS = 100;
	private int CACHE_SIZE = 10;
	private String CACHE_POLICY = "LRU";
	
	public void setUp() {
		kvServer = new KVServer(50005, CACHE_SIZE, CACHE_POLICY, "Node");
		kvClient = new KVStore("localhost", 50005);
        
        // Start the KVServer in a new thread
        new Thread(new Runnable() {
            @Override
            public void run() {
                kvServer.run();
            }
        }).start();
        // Thread.sleep(100); // Buffer in some time to start up
		
        try { 
			kvClient.connect();
		} catch (Exception e) {
            System.out.println("HELLO");
		}
	}

	public void tearDown() {
		kvClient.disconnect();
        File file = new File("/homes/c/chenam14/ece419/m1/ms1-group-35/m1-stub/src/logger/kvstorage.txt");
        if (file.delete()) { 
            System.out.println("Deleted the file: " + file.getName());
        } else {
            System.out.println("Failed to delete the file.");
        }
        kvServer.kill();
	}
	
	
	@Test
	public void test80Put20Get() {
		
		KVMessage response = null;
		Exception ex = null;

        long startTime = System.nanoTime();

        for (int i = 0; i < NUM_OPS * 0.8; i++) {
            try {
                response = kvClient.put("key" + i, "value" + i);
                // assertEquals(StatusType.PUT_SUCCESS, response.getStatus());
            } catch (Exception e) {
                ex = e;
                System.out.println("HELLO");
            }
        }
        for (int i = 0; i < NUM_OPS * 0.2; i++) {
            try {
                response = kvClient.get("key" + i);
                // assertEquals(StatusType.GET_SUCCESS, response.getStatus());
            } catch (Exception e) {
                ex = e;
                System.out.println("HELLO");
            }
        }
        
        long endTime = System.nanoTime();
        float seconds = ((float) endTime - (float) startTime) / 1000000000;
		
        System.out.println(NUM_OPS + " operations with 80% PUT, 20% GET: " + (seconds) + " seconds");

        // assertNull(ex);
        // assertEquals(StatusType.GET_SUCCESS, response.getStatus());
	}
	
	// @Test
	// public void test50Put50Get() {
		
	// 	KVMessage response = null;
	// 	Exception ex = null;

    //     long startTime = System.nanoTime();

    //     for (int i = 0; i < NUM_OPS * 0.5; i++) {
    //         try {
    //             response = kvClient.put("key" + i, "value" + i);
    //             // assertEquals(StatusType.PUT_SUCCESS, response.getStatus());
    //         } catch (Exception e) {
    //             ex = e;
    //         }
    //     }
    //     for (int i = 0; i < NUM_OPS * 0.5; i++) {
    //         try {
    //             response = kvClient.get("key" + i);
    //             // assertEquals(StatusType.GET_SUCCESS, response.getStatus());
    //         } catch (Exception e) {
    //             ex = e;
    //         }
    //     }
        
    //     long endTime = System.nanoTime();
    //     float seconds = ((float) endTime - (float) startTime) / 1000000000;
		
    //     System.out.println(NUM_OPS + " operations with 50% PUT, 50% GET: " + (seconds) + " seconds");

    //     assertNull(ex);
    //     // assertEquals(StatusType.GET_SUCCESS, response.getStatus());
	// }
	
    // @Test
	// public void test20Put80Get() {
		
	// 	KVMessage response = null;
	// 	Exception ex = null;

    //     long startTime = System.nanoTime();

    //     for (int i = 0; i < NUM_OPS * 0.2; i++) {
    //         try {
    //             response = kvClient.put("key" + i, "value" + i);
    //             assertEquals(StatusType.PUT_SUCCESS, response.getStatus());
    //         } catch (Exception e) {
    //             ex = e;
    //         }
    //     }
    //     for (int i = 0; i < NUM_OPS * 0.2; i++) {
    //         for (int j = 0; j < 4; j++) {
    //             try {
    //                 response = kvClient.get("key" + i);
    //                 assertEquals(StatusType.GET_SUCCESS, response.getStatus());
    //             } catch (Exception e) {
    //                 ex = e;
    //             }
    //         }
    //     }
        
    //     long endTime = System.nanoTime();
    //     float seconds = ((float) endTime - (float) startTime) / 1000000000;
		
    //     System.out.println(NUM_OPS + " operations with 20% PUT, 80% GET: " + (seconds) + " seconds");

    //     // assertNull(ex);
    //     // assertEquals(StatusType.GET_SUCCESS, response.getStatus());
	// }

}
