package testing;

import org.junit.Test;
import app_kvServer.KVServer;
import client.KVStore;
import app_kvECS.ECSClient;

import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import java.io.File;
import java.io.FilenameFilter;


public class M3Tests extends TestCase {
    private ECSClient ecsClient;
    private KVStore kvClient;

    @Override
    public void setUp() {
        try {
            kvClient = new KVStore("localhost", 50002);
            kvClient.connect();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void tearDown() {
        if (kvClient != null) {
            kvClient.disconnect();
        }
    }

    @Test
    public void testReplicationSuccess() {
        try {
            // Attempt to put a key-value pair
            KVMessage response = kvClient.put("testKey", "testValue");
            assertNotNull(response);
            assertEquals(StatusType.PUT_SUCCESS, response.getStatus());

            kvClient.disconnect();
            kvClient = new KVStore("localhost", 50003);
            kvClient.connect();
            response = kvClient.get("testKey");
            assertNotNull(response);
            assertEquals(StatusType.GET_SUCCESS, response.getStatus());

        } catch (Exception e) {
        }
    }

    @Test
    public void testReplicationConsistency() {
        try {
            // Attempt to put a key-value pair
            KVMessage response = kvClient.put("testKey2", "testValue2");
            assertNotNull(response);
            assertEquals(StatusType.PUT_SUCCESS, response.getStatus());

            kvClient.disconnect();
            kvClient = new KVStore("localhost", 50003);
            kvClient.connect();
            KVMessage response1 = kvClient.get("testKey2");
            assertNotNull(response1);

            kvClient.disconnect();
            kvClient = new KVStore("localhost", 50004);
            kvClient.connect();
            KVMessage response2 = kvClient.get("testKey2");
            assertNotNull(response2);
            
            assertEquals(response2.getValue(), response1.getValue());

        } catch (Exception e) {
        }
    }

    @Test
    public void testPutUpdateReplicationConsistency() {
        try {
            // Attempt to put a key-value pair
            KVMessage response = kvClient.put("testKey3", "testValue");
            assertNotNull(response);
            assertEquals(response.getStatus(), StatusType.PUT_SUCCESS);

            response = kvClient.put("testKey3", "testValue3");
            assertNotNull(response);
            assertEquals(response.getStatus(), StatusType.PUT_UPDATE);

            kvClient.disconnect();
            kvClient = new KVStore("localhost", 50003);
            kvClient.connect();
            KVMessage response1 = kvClient.get("testKey3");
            assertNotNull(response1);

            kvClient.disconnect();
            kvClient = new KVStore("localhost", 50004);
            kvClient.connect();
            KVMessage response2 = kvClient.get("testKey3");
            assertNotNull(response2);
            
            assertEquals(response2.getValue(), response1.getValue());

        } catch (Exception e) {
        }
    }
}