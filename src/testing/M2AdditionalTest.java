package testing;

import org.junit.Test;
import app_kvServer.KVServer;
import client.KVCommunication;
import client.KVStore;
import ecs.ConsistentHashing;
import app_kvECS.ECSClient;

import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.SimpleKVMessage;

import java.io.File;


import java.util.concurrent.CountDownLatch;


public class M2AdditionalTest extends TestCase {
    private ECSClient ecsClient;
    private KVStore kvClient;
    private KVServer kvServer;

    private CountDownLatch ecsLatch = new CountDownLatch(1);
    private CountDownLatch serverLatch = new CountDownLatch(1);
    
    private int NUM_OPS = 100;
    private int CACHE_SIZE = 10;
    private String CACHE_POLICY = "FIFO";

    @Override
    public void setUp() {
        ecsClient = new ECSClient(51000);
        new Thread(new Runnable() {
            @Override
            public void run() {
                ecsClient.startListening();
            }
        }).start();

        kvServer = new KVServer(50001, CACHE_SIZE, CACHE_POLICY, "Node_1");
        new Thread(new Runnable() {
            @Override
            public void run() {
                kvServer.run();
            }
        }).start();

        try {
            Thread.sleep(1000); 
            kvClient = new KVStore("localhost", 50001);
            kvClient.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void tearDown() {
        try {
            if (kvClient != null) {
                kvClient.disconnect();
            }
            if (kvServer != null) {
                kvServer.kill();
            }
            if (ecsClient != null) {
                ecsClient.stopListening();
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail("Teardown failed: " + e.getMessage());
        }
    }

    @Test
    public void testKeyRange() {
        KVMessage response = null;

        try {
            response = kvClient.keyrange();
            assertNotNull("Response is null", response);
            assertEquals("KEYRANGE operation failed", StatusType.KEYRANGE_SUCCESS, response.getStatus());
        } catch (Exception e) {
            fail("Exception during KEYRANGE operation: " + e.getMessage());
        }
    }
        
}
