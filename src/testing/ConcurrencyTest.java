package testing;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import app_kvServer.KVServer;
import junit.framework.TestCase;

public class ConcurrencyTest extends TestCase {
	
	private KVServer kvServer;

	@Before
    public void setUp() throws Exception {
        kvServer = new KVServer(50005, 10, "FIFO");
    }

    @After
    public void tearDown() throws Exception {
        kvServer.close();
    }

    @Test
    public void testConcurrentPuts() throws InterruptedException {
        ExecutorService service = Executors.newFixedThreadPool(5);
        final int numPuts = 10;
        
        // Concurrent Put operations
        for (int i = 0; i < numPuts; i++) {
            final String key = "key" + i;
            final String value = "value" + i;
            service.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        kvServer.putKV(key, value);
                    } catch (Exception e) {
                        fail("Failed to put value! Error: " + e.getMessage());
                    }
                }
            });
        }

        service.shutdown();
        assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));

        // Test the puts were done correctly
        for (int i = 0; i < numPuts; i++) {
            final String key = "key" + i;
            try {
                String returnValue = kvServer.getKV(key);
                assertEquals("value" + i, returnValue);
            } catch (Exception e) {
                fail("Failed to get value for key '" + key + "'! Error: " + e.getMessage());
            }
        }
    }

    @Test
    public void testConcurrentGets() throws Exception {
        // Setup with the key-value pairs
        final int numGets = 10;
        for (int i = 0; i < numGets; i++) {
            kvServer.putKV("key" + i, "value" + i);
        }

        // Concurrent get operations
        ExecutorService service = Executors.newFixedThreadPool(5);
        for (int i = 0; i < numGets; i++) {
            final String key = "key" + i;
            service.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        String returnValue = kvServer.getKV(key);
                        assertEquals("value" + key.substring(3), returnValue);
                    } catch (Exception e) {
                        fail("Failed to get value! Error: " + e.getMessage());
                    }
                }
            });
        }

        service.shutdown();
        assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));
    }

    @Test
    public void testConcurrentPutAndGet() throws InterruptedException {
        ExecutorService service = Executors.newFixedThreadPool(10);
        final int numOps = 20;

        for (int i = 0; i < numOps; i++) {
            final String key = "key";
            final String value = "value" + i;
            service.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        kvServer.putKV(key, value);
                    } catch (Exception e) {
                        fail("Failed to put value! Error: " + e.getMessage());
                    }
                }
            });
            service.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        kvServer.getKV(key);
                    } catch (Exception e) {
                        fail("Failed to get value! Error: " + e.getMessage());
                    }
                }
            });
        }

        service.shutdown();
        assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));

        // Final check to ensure the key exists
        try {
            assertNotNull(kvServer.getKV("key"));
        } catch (Exception e) {
            fail("Failed to get value! Error: " + e.getMessage());
        }
        
    }

    @Test
    public void testConcurrentDeleteAndGet() throws InterruptedException {
        final String key = "key";
        final String value = "value";
        
        // Setup with the key-value pair
        try {
            kvServer.putKV(key, value);
        } catch (Exception e) {
            fail("Setup failed! Error: " + e.getMessage());
        }

        ExecutorService service = Executors.newFixedThreadPool(2);

        // First thread: Delete the value
        service.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    kvServer.putKV(key, null);
                } catch (Exception e) {
                    fail("Deletion failed! Error: " + e.getMessage());
                }
            }
        });

        // Second thread: Attempt to get the value
        service.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    String retrievedValue = kvServer.getKV(key);
                    assertNull("Expected null but got: " + retrievedValue, retrievedValue);
                } catch (Exception e) {
                    fail("Retrieval failed: " + e.getMessage());
                }
            }
        });

        service.shutdown();
        assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));
    }
}
