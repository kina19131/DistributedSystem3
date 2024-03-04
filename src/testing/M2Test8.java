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


public class M2Test8 extends TestCase {
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
    

    //SUCCESS: 6 REBOOT LAST ONE STANDING: reboot kvS_last; will it still have all the data? (Persistent)
    @Test
    public void testRebootLastStandingServer() {
        try {
            // Putting a key-value pair before rebooting the server
            String key = "key";
            String value = "value";
            KVMessage response = kvClient.put(key, value);
            assertEquals("PUT operation", StatusType.PUT_SUCCESS, response.getStatus());
    
            // Simulating server reboot
            if (kvServer1 != null) {
                kvServer1.kill(); 
                Thread.sleep(2000); // Giving some time for the server to shut down
            }
            
            // Restarting the server
            kvServer1 = new KVServer(50000, CACHE_SIZE, CACHE_POLICY, "Node_1");
            new Thread(new Runnable() {
                @Override
                public void run() {
                    kvServer1.run(); 
                }
            }).start();
            Thread.sleep(5000); // Waiting for the server to be fully operational
            
            // Fetching the value for the key to verify persistence
            response = kvClient.get(key);
            assertEquals("Data should persist through reboot.", value, response.getValue()); 
        } catch (Exception e) {
            fail("Exception during rebooting the last standing server: " + e.getMessage());
        }
    }
}

