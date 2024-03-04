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


public class M2Test2 extends TestCase {
    private ECSClient ecsClient;
    private KVStore kvClient;
    private KVServer kvServer1;
    private KVServer kvServer2;
    private KVServer kvServer3; 

    private int CACHE_SIZE = 10;
    private String CACHE_POLICY = "FIFO";

    @Override
    public void setUp() {
        try {
            ecsClient = new ECSClient(51000);
            new Thread(new Runnable() {
                @Override
                public void run() {
                    ecsClient.startListening();
                }
            }).start();

            Thread.sleep(3000);

            kvServer1 = new KVServer(50000, CACHE_SIZE, CACHE_POLICY, "Node_1");
            new Thread(new Runnable() {
                @Override
                public void run() {
                    kvServer1.run();
                }
            }).start();

            Thread.sleep(3000);

            kvClient = new KVStore("localhost", 50000);
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
        if (kvServer1 != null) {
            kvServer1.kill();
        }
        if (kvServer2 != null) {
            kvServer2.kill();
        }
        if (kvServer3 != null) {
            kvServer3.kill();
        }
        if (ecsClient != null) {
            ecsClient.stopListening();
        }
        // Specify the directory where the files are located
        File dir = new File(".");

        // Filter to identify files that match the pattern kvstorage_*.txt
        FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("kvstorage_") && name.endsWith(".txt");
            }
        };

        // List all files that match the filter
        File[] files = dir.listFiles(filter);

        // Delete each file that matches the pattern
        if (files != null) {
            for (File file : files) {
                if (file.delete()) {
                    System.out.println("Deleted the file: " + file.getName());
                } else {
                    System.out.println("Failed to delete the file: " + file.getName());
                }
            }
        } else {
            System.out.println("No files found matching the pattern.");
        }
    }



    @Test
    public void testServerAddition() {
        final String key = "keyForNewServer";
        final String value = "valueForNewServer";
        KVMessage response;

        // Initial PUT operation
        try {
            response = kvClient.put(key, value);
            assertEquals("PUT operation: ", StatusType.PUT_SUCCESS, response.getStatus());
        } catch (Exception e) {
            fail("Failed to PUT: " + e.getMessage());
        }

        // Start a new server
        try {
            kvServer2 = new KVServer(50001, CACHE_SIZE, CACHE_POLICY, "Node_2");
            new Thread(new Runnable() {
                @Override
                public void run() {
                    kvServer2.run();
                }
            }).start();

            Thread.sleep(3000);
        } catch (Exception e){

        }

        // Check ECSClient for the new server addition
        assertTrue("ECSClient should have more than 1 node after adding new server", ecsClient.getNodes().size() > 1);

        // Verify that the key-value pair is accessible after server addition
        try {
            response = kvClient.get(key);
            assertEquals("GET operation: ", StatusType.GET_SUCCESS, response.getStatus());
            assertEquals("Verify value: ", value, response.getValue());
            System.out.println("Retrieved value: " + response.getValue());
        } catch (Exception e) {
            fail("Failed to GET: " + e.getMessage());
        }
    }
}