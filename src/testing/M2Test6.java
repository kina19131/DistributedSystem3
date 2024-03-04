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


public class M2Test6 extends TestCase {
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

            Thread.sleep(500);

            kvServer1 = new KVServer(50000, CACHE_SIZE, CACHE_POLICY, "Node_1");
            new Thread(new Runnable() {
                @Override
                public void run() {
                    kvServer1.run();
                }
            }).start();

            Thread.sleep(500);

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
            ecsClient.stop();
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
    

    // 4 REMOVE: shutdown some kv servers. Similar to ADD, Will the hash ring be updated with proper data transfer?
    @Test
    public void testServerRemovalAndDataRebalance() {
        try {
            // Ensure there's data in the system
            String key = "removalTestKey";
            String value = "removalTestValue";
            KVMessage response = kvClient.put(key, value);
            assertNotNull(response);

            System.out.println("LOOOOOOOOOOK...1, size: "+ ecsClient.getNodes().size());

            kvServer3 = new KVServer(50002, CACHE_SIZE, CACHE_POLICY, "Node_2");
            new Thread(new Runnable() {
                @Override
                public void run() {
                    kvServer3.start();
                }
            }).start();
            Thread.sleep(500); // Wait for change propagation
            
            // Remove a server and simulate ECS updating the hash ring
            if (kvServer3 != null){
                kvServer3.kill();
                Thread.sleep(500); // Give time for server shutdown and ECS update
                System.out.println("LOOOOOOOOOOK...2 , successfully removed");
            }

            System.out.println("LOOOOOOOOOOK...3, size: "+ ecsClient.getNodes().size());
            System.out.println("LOOOOOOOOOOK...4:" + ecsClient.getNodes().entrySet().toString());

            
            // Verify the hash ring is updated and data is correctly accessible
            response = kvClient.get(key);
            assertNotNull(response);
        } catch (Exception e) {
        }
    }
}




