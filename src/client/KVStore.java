package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import client.KVCommunication;
import ecs.ConsistentHashing;
import app_kvECS.ECSClient;

import shared.messages.KVMessage;
import shared.messages.SimpleKVMessage;
import shared.messages.KVMessage.StatusType;

public class KVStore implements KVCommInterface {

	private Logger logger = Logger.getRootLogger();
	private boolean running;
	
	private String serverAddress;
	private int serverPort;

	private String metadata;

	private KVCommunication kvComm;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		serverAddress = address;
		serverPort = port;
		logger.info("KVStore initialized.");
	}

	@Override
	public void connect() throws UnknownHostException, Exception {
		if (serverAddress == null || serverPort <= 0) {
            throw new IllegalStateException("Server address and port are not set.");
        }
        kvComm = new KVCommunication(serverAddress, serverPort);
        kvComm.connect();
        setRunning(true);
	}

	@Override
	public void disconnect() {
		if (isRunning()) {
			kvComm.closeConnection();
			setRunning(false);
		}
	}


	@Override
	public KVMessage put(String key, String value) throws SocketException, Exception {
		// logger.info("Sending PUT request for key: " + key + " with value: " + value);
		System.out.println("Sending PUT request for key: " + key + " with value: " + value);
		KVMessage response = sendMessageWithRetry(StatusType.PUT, key, value);
		if (response != null) {
			logger.info("Received PUT response: " + response.getStatus() + " for key: " + response.getKey());
		} else {
			logger.error("Received null response for PUT request for key: " + key);
		}
		return response;
	}

	@Override
	public KVMessage get(String key) throws SocketException, Exception {
		logger.info("Sending GET request for key: " + key); // Log the sending of GET request
		SimpleKVMessage requestResponse = sendMessageWithRetry(StatusType.GET, key, null); // Send the GET request and immediately wait for the response
		if (requestResponse != null) {
			logger.info("Received GET response: " + requestResponse.getStatus() + " for key: " + requestResponse.getKey() + " with value: " + requestResponse.getValue()); // Log the received response
		} else {
			logger.error("Received null response for GET request for key: " + key); // Log error if response is null
		}
		return requestResponse; // Return the response
	}

	private SimpleKVMessage sendMessageWithRetry(StatusType status, String key, String value) throws SocketException, Exception {
		SimpleKVMessage response = kvComm.sendMessage(status, key, value);
		if (response != null && response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
			// Find the responsible server
			SimpleKVMessage keyrangeRes = keyrange();
			String[] keyrangeResMsg = keyrangeRes.getMsg().split(" ", 2);
			metadata = keyrangeResMsg[1];
			String newHost = findResponsibleServer(metadata, key);
			if (newHost != null) {
				String[] newHostDetails = newHost.split(":");
				String newHostIP = newHostDetails[0];
				Integer newHostPort = Integer.parseInt(newHostDetails[1]);

				// TODO: Retry connection to correct server
				reconnect(newHostIP, newHostPort);
				response = kvComm.sendMessage(status, key, value);
			}
		}
		return response;
	}

	public SimpleKVMessage keyrange() throws SocketException, Exception {
		SimpleKVMessage response = kvComm.sendMessage(StatusType.KEYRANGE, null, null);
		return response;
	}

	public SimpleKVMessage keyrange_read() throws SocketException, Exception {
		SimpleKVMessage response = kvComm.sendMessage(StatusType.KEYRANGE_READ, null, null);
		return response;
	}

	/* Parse metadata string to find the responsible server */
	private static String findResponsibleServer(String metadata, String key) {
		String[] nodes = metadata.split(";");
		String keyHash = ConsistentHashing.getKeyHash(key);
		// String keyHash = key;
		
		for (String node : nodes) {
			String[] nodeDetails = node.split(",");
			String nodeHost = nodeDetails[2];

			String[] hashrange = {nodeDetails[0], nodeDetails[1]};
			
			if (ConsistentHashing.isKeyInRange(keyHash, hashrange)) {
				return nodeHost;
			}
		}
		return null;
	}

	public void reconnect(String address, int port) throws SocketException, Exception {
		logger.info("Disconnecting from server: " + serverAddress + ":" + Integer.toString(serverPort));
		disconnect();
		serverAddress = address;
		serverPort = port;
		logger.info("Connecting to server: " + serverAddress + ":" + Integer.toString(serverPort));
		connect();
	}

	public void setRunning(boolean run) {
		running = run;
	}

	public boolean isRunning() {
		return running;
	}

}