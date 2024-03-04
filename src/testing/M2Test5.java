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


public class M2Test5 extends TestCase {
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



    // 3 ADD: start more kv servers. Will the ECS update the hash ring correctly and new servers can 
    // find their positions? Will the data be transferred correctly to the right server? (Check it through PUTs/GETs from the client)
    @Test
    public void testServerAdditionAndDataRebalance() {
        try {
            // Initial PUT operation
            String key = "initialKey";
            String value = "initialValue";
            KVMessage response = kvClient.put(key, value);
            assertEquals("Initial PUT operation", StatusType.PUT_SUCCESS, response.getStatus());
    
            // Start a new KV servers
            kvServer2 = new KVServer(50001, CACHE_SIZE, CACHE_POLICY, "Node_2");
            new Thread(new Runnable() {
                @Override
                public void run() {
                    kvServer2.start();
                }
            }).start();
            Thread.sleep(5000); // Wait for the server to start and ECS to integrate it
    
            kvServer3 = new KVServer(50002, CACHE_SIZE, CACHE_POLICY, "Node_2");
            new Thread(new Runnable() {
                @Override
                public void run() {
                    kvServer3.start();
                }
            }).start();
            Thread.sleep(5000); // Wait for the server to start and ECS to integrate it
    
       
             // Verify that data is correctly accessible and rebalanced 
             // If rebalance fails, the errors will be triggered
            response = kvClient.get(key);
            assertEquals("GET operation", StatusType.GET_SUCCESS, response.getStatus());
            assertEquals("Data should be correctly rebalanced and accessible:", value, response.getValue());

        } catch (Exception e) {
            fail("Exception during server addition and rebalance test: " + e.getMessage());
        }
    }
}