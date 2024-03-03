package testing;

import org.junit.Test;
import app_kvServer.KVServer;
import client.KVStore;
import ecs.ConsistentHashing;
import app_kvECS.ECSClient;

import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.SimpleKVMessage;

import java.io.File;


import java.util.concurrent.CountDownLatch;





public class M2BasicTest extends TestCase {
    private KVStore kvClient;
    private KVServer kvServer;
    
    private int NUM_OPS = 100;
    private int CACHE_SIZE = 10;
    private String CACHE_POLICY = "FIFO";

    @Override
    public void setUp() {
        kvServer = new KVServer(50000, CACHE_SIZE, CACHE_POLICY, "Node_1");
        kvClient = new KVStore("localhost", 50000);
        
        try {
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
        } catch (Exception e) {
            e.printStackTrace();
            fail("Teardown failed: " + e.getMessage());
        }
    }



    @Test
    public void testKeyHash() {
        String key = "foo2";
        String keyHash = ConsistentHashing.getKeyHash(key);

        assertEquals("KeyHash operation failed", keyHash, "92e0057157f69e22a364d6b22dd6bbd5");
    }

    @Test
    public void testNoECS() {
        String key = "foo2";
        String value = "bar2";
        KVMessage response = null;

        try {
            response = kvClient.put(key, value);
            assertNotNull("Response is null", response);
            assertEquals("PUT operation failed", StatusType.SERVER_STOPPED, response.getStatus());
        } catch (Exception e) {
            fail("Exception during PUT operation: " + e.getMessage());
        }
    }
    
}
