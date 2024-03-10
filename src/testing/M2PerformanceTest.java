package testing;

import org.junit.Test;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import client.KVStore;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.SimpleKVMessage;

import static java.lang.Thread.sleep;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class M2PerformanceTest extends TestCase {

	private List<KVStore> kvClients = new ArrayList<>();
    private KVStore kvClient;
    
    private Map<String, String> dataMap = new HashMap<>();
    private List<String> keys = new ArrayList<>();

    private int NUM_OPS = 100;
    private int NUM_SERVERS =20;
    private int NUM_CLIENTS = 10;
    
    private int BASE_PORT = 50000;
	
	public void setUp() {

        // Data setup
        String csvFile = "/homes/c/chenam14/ece419/extracted_info.csv";
        String line = "";
        String csvSplitBy = ",";

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            int i = 0;
            while ((line = br.readLine()) != null) {
                // use comma as separator
                String[] emailInfo = line.split(csvSplitBy);
                dataMap.put("key"+i, emailInfo[0]);
                keys.add("key"+i);
                i++;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        // Start KV clients
        for (int i = 0; i < NUM_CLIENTS; i++) {
            kvClient = new KVStore("localhost", 50000);
            try {
                kvClient.connect();
                kvClients.add(kvClient);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.println("DONE SETUP");
	}

	public void tearDown() {
		for (KVStore client : kvClients) {
            client.disconnect();
        }
	}
	
	
	@Test
	public void test80Put20Get() {
		
		KVMessage response = null;
        Exception ex = null;

        long startTime = System.nanoTime();

        // Distribute PUT operations across all clients
        for (int i = 0; i < NUM_OPS * 0.8; i++) {
            KVStore client = kvClients.get((i % NUM_CLIENTS));
            try {
                String key = keys.get(i);
                String value = dataMap.get(key);
                response = client.put(key, value);
                // Perform assertion or validation if necessary
                assertEquals(StatusType.PUT_SUCCESS, response.getStatus());
            } catch (Exception e) {
                ex = e;
                e.printStackTrace();
            }
        }

        // Distribute GET operations across all clients
        for (int i = 0; i < NUM_OPS * 0.2; i++) {
            KVStore client = kvClients.get((i % NUM_CLIENTS));
            try {
                String key = keys.get(i);
                response = client.get(key);
                // Perform assertion or validation if necessary
                assertEquals(StatusType.GET_SUCCESS, response.getStatus());
            } catch (Exception e) {
                ex = e;
                e.printStackTrace();
            }
        }

        long endTime = System.nanoTime();
        float seconds = ((float) endTime - (float) startTime) / 1000000000;
        // float seconds = ((float) endTime - (float) startTime);

        System.out.println(NUM_OPS + " operations with 80% PUT, 20% GET across all clients: " + seconds + " seconds");
	}
	
	@Test
	public void test50Put50Get() {
		
		KVMessage response = null;
        Exception ex = null;

        long startTime = System.nanoTime();

        // Distribute PUT operations across all clients
        for (int i = 0; i < NUM_OPS * 0.5; i++) {
            KVStore client = kvClients.get((i % NUM_CLIENTS));
            try {
                String key = keys.get(i);
                String value = dataMap.get(key);
                response = client.put(key, value);
                // Perform assertion or validation if necessary
                assertEquals(StatusType.PUT_SUCCESS, response.getStatus());
            } catch (Exception e) {
                ex = e;
                e.printStackTrace();
            }
        }

        // Distribute GET operations across all clients
        for (int i = 0; i < NUM_OPS * 0.5; i++) {
            KVStore client = kvClients.get((i % NUM_CLIENTS));
            try {
                String key = keys.get(i);
                response = client.get(key);
                // Perform assertion or validation if necessary
                assertEquals(StatusType.GET_SUCCESS, response.getStatus());
            } catch (Exception e) {
                ex = e;
                e.printStackTrace();
            }
        }

        long endTime = System.nanoTime();
        float seconds = ((float) endTime - (float) startTime) / 1000000000;
        // float seconds = ((float) endTime - (float) startTime);

        System.out.println(NUM_OPS + " operations with 50% PUT, 50% GET across all clients: " + seconds + " seconds");
	}

    @Test
	public void test20Put80Get() {
		
		KVMessage response = null;
        Exception ex = null;

        long startTime = System.nanoTime();

        // Distribute PUT operations across all clients
        for (int i = 0; i < NUM_OPS * 0.5; i++) {
            KVStore client = kvClients.get((i % NUM_CLIENTS));
            try {
                String key = keys.get(i);
                String value = dataMap.get(key);
                response = client.put(key, value);
                // Perform assertion or validation if necessary
                assertEquals(StatusType.PUT_SUCCESS, response.getStatus());
            } catch (Exception e) {
                ex = e;
                e.printStackTrace();
            }
        }

        int k = 0;
        // Distribute GET operations across all clients
        for (int i = 0; i < NUM_OPS * 0.2; i++) {
            for (int j = 0; j < 4; j++) {
            
                KVStore client = kvClients.get((k % NUM_CLIENTS));
                k++;
                try {
                    String key = keys.get(i);
                    response = client.get(key);
                    // Perform assertion or validation if necessary
                    assertEquals(StatusType.GET_SUCCESS, response.getStatus());
                } catch (Exception e) {
                    ex = e;
                    e.printStackTrace();
                }
            }
        }

        long endTime = System.nanoTime();
        float seconds = ((float) endTime - (float) startTime) / 1000000000;
        // float seconds = ((float) endTime - (float) startTime);

        System.out.println(NUM_OPS + " operations with 50% PUT, 50% GET across all clients: " + seconds + " seconds");
	}

}
