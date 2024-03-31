package testing;

import org.junit.Test;
import app_kvServer.KVServer;
import client.KVCommunication;
import client.KVStore;
import ecs.ConsistentHashing;
import app_kvECS.ECSClient;
import shared.metadata.Metadata;

import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.SimpleKVMessage;

import java.beans.Transient;
import java.io.File;

public class M3AdditionalTest extends TestCase {
    private ECSClient ecsClient;
    private KVStore kvClient;

    @Override
    public void setUp() {
        try {
            kvClient = new KVStore("localhost", 50002);
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
        } catch (Exception e) {
            e.printStackTrace();
            fail("Teardown failed: " + e.getMessage());
        }
    }

    @Test
    public void testMetadataUpdate() {
        Metadata metadata = new Metadata();
        String metadataStr = "2b786438d2c6425dc30de0077ea6494d,2b786438d2c6425dc30de0077ea6494d,localhost:50000;";
        metadata.setMetadata(metadataStr);
        assertEquals(metadataStr, metadata.getMetadataString());
    }

    @Test
    public void testIsServerResponsible() {
        Metadata metadata = new Metadata();
        String metadataStr = "2b786438d2c6425dc30de0077ea6494d,2b786438d2c6425dc30de0077ea6494d,localhost:50000;";
        metadata.setMetadata(metadataStr);
        
        String key = "key";
        Boolean serverResponsible = metadata.isServerResponsible("localhost:50000", key);
        assertTrue(serverResponsible);
    }

    @Test
    public void testFindResponsibleServer() {
        Metadata metadata = new Metadata();
        String metadataStr = "2b786438d2c6425dc30de0077ea6494d,2b786438d2c6425dc30de0077ea6494d,localhost:50000;";
        metadata.setMetadata(metadataStr);
        
        String key = "key";
        String serverResponsible = metadata.findResponsibleServer(key);
        assertEquals("localhost:50000", serverResponsible);
    }

    @Test
    public void testKeyRangeRead() {
        KVMessage response = null;

        try {
            response = kvClient.keyrange_read();
            assertNotNull("Response is null", response);
            assertEquals("KEYRANGE operation failed", StatusType.KEYRANGE_READ_SUCCESS, response.getStatus());
        } catch (Exception e) {
            fail("Exception during KEYRANGE operation: " + e.getMessage());
        }
    }
        
}
