package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import client.KVCommunication;

import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

public class KVStore implements KVCommInterface {

	private Logger logger = Logger.getRootLogger();
	private boolean running;
	
	private String serverAddress;
	private int serverPort;

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
	public KVMessage put(String key, String value) throws Exception {
		logger.info("Sending PUT request for key: " + key + " with value: " + value);
		KVMessage response = kvComm.sendMessage(StatusType.PUT, key, value);
		if (response != null) {
			logger.info("Received PUT response: " + response.getStatus() + " for key: " + response.getKey());
		} else {
			logger.error("Received null response for PUT request for key: " + key);
		}
		return response;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		logger.info("Sending GET request for key: " + key); // Log the sending of GET request
		KVMessage requestResponse = kvComm.sendMessage(StatusType.GET, key, null); // Send the GET request and immediately wait for the response
		if (requestResponse != null) {
			logger.info("Received GET response: " + requestResponse.getStatus() + " for key: " + requestResponse.getKey() + " with value: " + requestResponse.getValue()); // Log the received response
		} else {
			logger.error("Received null response for GET request for key: " + key); // Log error if response is null
		}
		return requestResponse; // Return the response
	}


	public void setRunning(boolean run) {
		running = run;
	}

	public boolean isRunning() {
		return running;
	}

}