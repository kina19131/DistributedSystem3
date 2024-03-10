package testing;

import org.junit.Test;
import junit.framework.TestCase;

import client.KVStore;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import java.io.File;
import java.net.UnknownHostException;

public class AdditionalTest extends TestCase {
	
	private KVStore kvClient;

    @Override
    public void setUp() {
        kvClient = new KVStore("localhost", 50000);
        try {
            kvClient.connect();
        } catch (Exception e) {
        }
    }

    @Override
    public void tearDown() {
        kvClient.disconnect();

        File file = new File("./kvstorage.txt");
        if (file.delete()) { 
            System.out.println("Deleted the file: " + file.getName());
        } else {
            System.out.println("Failed to delete the file.");
        }
    }
	
    // Test Case 1: Put a Long Key
	@Test
	public void testPutLongKey() {
		String key = "VeryLongKey";  
		String value = "longKeyTestValue";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}

    // Test Case 2: Put Special Character in Key
    @Test
    public void testPutSpecialCharacterKey() {
        String key = "!@#$%^&*()";
        String value = "specialCharacterKeyTestValue";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
    }

    // Test Case 3: Put Large Value
    @Test
	public void testPutLargeValue() {
		String key = "thisKey";
		String value = "A".repeat(1000);  
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}

    // Test Case 4: Value With Spaces
    @Test
	public void testValueWithSpaces() {
		String key = "this_key";
		String value = "this is value with space";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}

    // Test Case 5: Put Empty Value (Delete Key)
    @Test
    public void testPutEmptyValue() {
        String key = "empty_key";
        String value = "";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, "initial_value");
        } catch (Exception e) {
            ex = e;
        }

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
    }

    // Test Case 6: Put and Delete with Null Value
    @Test
    public void testPutAndDeleteNull() {
        String key = "test8";
        String value = "putAndDeleteTestValue";
        KVMessage putResponse = null;
        KVMessage deleteResponse = null;
        KVMessage getResponse = null;
        Exception ex = null;

        try {
            putResponse = kvClient.put(key, value);
            deleteResponse = kvClient.put(key, "null");
            getResponse = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && putResponse.getStatus() == StatusType.PUT_SUCCESS
                && deleteResponse.getStatus() == StatusType.DELETE_SUCCESS
                && getResponse.getStatus() == StatusType.GET_ERROR);
    }

    // Test Case 7: Get After Disconnect
    @Test
    public void testGetAfterDisconnect() {
        KVMessage response = null;
        Exception ex = null;
        String key = "key_testGetAfterDisconnect";
        String value = "value_testGetAfterDisconnect";
        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        // Step 1: Disconnect the client from the server
        kvClient.disconnect();

        Exception reconnectionEx = null;
        try {
            kvClient.connect();
        } catch (Exception e) {
            reconnectionEx = e;
        }

        assertTrue("Reconnection failed?", reconnectionEx == null);

        // Step 3: Attempt to get the key
        try {
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }
        assertTrue("Get operation failed after reconnection", ex == null && response.getStatus() == StatusType.GET_SUCCESS);
    }

    // Test Case 8: Put Same Value Multiple Times
    @Test
    public void testPutSameValueMultipleTimes() {
        String key = "repeatedValueKey";
        String value = "repeatedValue";
        KVMessage putResponse1 = null;
        KVMessage putResponse2 = null;
        KVMessage putResponse3 = null;
        Exception ex = null;

        try {
            putResponse1 = kvClient.put(key, value);
            putResponse2 = kvClient.put(key, value);
            putResponse3 = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && putResponse1.getStatus() == StatusType.PUT_SUCCESS
                && putResponse2.getStatus() == StatusType.PUT_UPDATE
                && putResponse3.getStatus() == StatusType.PUT_UPDATE);
    }
}
