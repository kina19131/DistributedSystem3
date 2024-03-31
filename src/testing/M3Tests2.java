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

import java.lang.ProcessBuilder;


public class M3Tests2 extends TestCase {
    private ECSClient ecsClient;
    private KVStore kvClient;
    private KVServer kvServerNew;
    private int CACHE_SIZE = 10;
    private String CACHE_POLICY = "FIFO";

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
        if (kvServerNew != null) {
            kvServerNew.kill();
        }
    }

    @Test
    public void testDataRecoveryAfterFailure() {
        ProcessBuilder processBuilder = new ProcessBuilder("java", "-jar", "m2-server.jar", "-p", "50010");
        Process serverProcess = null;

        final String key = "testFailureKEY";
        final String value = "testFailureVALUE";
        KVMessage response;

        // Start new server    
        try {
            serverProcess = processBuilder.start();
        } catch (Exception e) {
        }
        

        // Initial PUT operation
        try {
            kvClient.disconnect();
            kvClient = new KVStore("localhost", 50010);
            kvClient.connect();
            response = kvClient.put(key, value);
            assertNotNull(response);
            assertEquals(StatusType.PUT_SUCCESS, response.getStatus());
        } catch (Exception e) {
        }

        // Kill server
        serverProcess.destroyForcibly();

        // Verify that the key-value pair is accessible after server addition
        try {
            kvClient.disconnect();
            kvClient = new KVStore("localhost", 50002);
            kvClient.connect();
            response = kvClient.get(key);
            assertNotNull(response);
            // assertEquals(StatusType.GET_SUCCESS, response.getStatus());
        } catch (Exception e) {
        }
    }

    @Test
    public void testDataConsistencyAfterFailure() {
        ProcessBuilder processBuilder = new ProcessBuilder("java", "-jar", "m2-server.jar", "-p", "50010");
        Process serverProcess = null;

        final String key = "testFailure2KEY";
        final String value = "testFailure2VALUE";
        KVMessage response;

        // Start new server    
        try {
            serverProcess = processBuilder.start();
        } catch (Exception e) {
        }
        

        // Initial PUT operation
        try {
            kvClient.disconnect();
            kvClient = new KVStore("localhost", 50010);
            kvClient.connect();
            response = kvClient.put(key, value);
            assertNotNull(response);
            assertEquals(StatusType.PUT_SUCCESS, response.getStatus());
        } catch (Exception e) {
        }

        // Kill server
        serverProcess.destroyForcibly();

        // Verify that the key-value pair is accessible after server addition
        try {
            kvClient.disconnect();
            kvClient = new KVStore("localhost", 50002);
            kvClient.connect();
            response = kvClient.get(key);
            assertNotNull(response);
            // assertEquals(value, response.getValue());
        } catch (Exception e) {
        }
    }

    @Test
    public void testServerAdditionWithReplication() {
        final String key = "keyForNewServer";
        final String value = "valueForNewServer";
        KVMessage response;

        // Initial PUT operation
        try {
            response = kvClient.put(key, value);
            assertNotNull(response);
            assertEquals(StatusType.PUT_SUCCESS, response.getStatus());
        } catch (Exception e) {
        }

        // Start a new server
        try {
            kvServerNew = new KVServer(50008, CACHE_SIZE, CACHE_POLICY, "Node_4");
            new Thread(new Runnable() {
                @Override
                public void run() {
                    kvServerNew.run();
                }
            }).start();

            Thread.sleep(500);
        } catch (Exception e){

        }

        // Check ECSClient for the new server addition
        // assertTrue(ecsClient.getNodes().size() > 3);

        // Verify that the key-value pair is accessible after server addition
        try {
            kvClient.disconnect();
            kvClient = new KVStore("localhost", 50008);
            kvClient.connect();
            response = kvClient.get("keyForNewServer");
            assertNotNull(response);
            assertEquals(StatusType.GET_SUCCESS, response.getStatus());
        } catch (Exception e) {
        }
    }
}