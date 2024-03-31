package app_kvServer;

import shared.messages.SimpleKVMessage;
import shared.messages.KVMessage.StatusType;

import java.io.PushbackInputStream;


import java.net.Socket;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.IOException;
import java.net.SocketException;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.io.InputStream;
import java.io.OutputStream;

import shared.messages.KVMessage;
import shared.messages.SimpleKVCommunication;
import shared.metadata.Metadata;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;

import ecs.ConsistentHashing;

public class ClientHandler implements Runnable {
    private Socket clientSocket;
    private KVServer server; 
    private boolean isOpen;
    private PushbackInputStream input; // Change to PushbackInputStream
    private OutputStream output;

    private static final Logger LOGGER = Logger.getRootLogger();

    private String[] nodeHashRange;

    // Constructor now accepts a PushbackInputStream
    public ClientHandler(Socket socket, KVServer server, String[] keyRange, PushbackInputStream input) {
        this.clientSocket = socket;
        this.server = server; 
        this.isOpen = true;
        this.input = input; // Use the provided PushbackInputStream
        this.nodeHashRange = keyRange; 

        try {
            this.output = clientSocket.getOutputStream(); // Initialize output stream here
        } catch (IOException e) {
            LOGGER.error("Error initializing client handler I/O", e);
        }
    }

    @Override
    public void run() {
        System.out.println("... REACHED CLIENT HANDLER ... 1");
        try {
            System.out.println("ClientHandler, INPUT:" + input);
            while (isOpen) {
                try {
                    System.out.println("... REACHED CLIENT HANDLER ... 2");
                    SimpleKVMessage responseMessage = null;

                    String msg = SimpleKVCommunication.receiveMessage(input, LOGGER);
                    System.out.println("ClientHandler received msg:" + msg);
                    SimpleKVMessage requestMessage = SimpleKVCommunication.parseMessage(msg, LOGGER);
      
                    // If server doesn't have node hash range
                    if (nodeHashRange[0] == null && nodeHashRange[1] == null) {
                        responseMessage = new SimpleKVMessage(StatusType.SERVER_STOPPED, null);
                        SimpleKVCommunication.sendMessage(responseMessage, output, LOGGER);
                    
                    // Updating Status during metadatat update (rebalance) - SERVER_WRITE_LOCK
                    } else if (requestMessage.getStatus() == StatusType.PUT && !server.canWrite()){
                        responseMessage = new SimpleKVMessage(StatusType.SERVER_WRITE_LOCK, null);
                        System.out.println("SERVER_WRITE_LOCK TRIGGERD");
                        SimpleKVCommunication.sendMessage(responseMessage, output, LOGGER);
    
                    // Keyrange request
                    } else if (requestMessage.getStatus() == StatusType.KEYRANGE){
                        try {
                            String response = server.keyrange();
                            responseMessage = new SimpleKVMessage(StatusType.KEYRANGE_SUCCESS, response);
                            LOGGER.info("Processed keyrange request and returned: " + requestMessage.getMsg());
                        } catch (Exception e) {
                            LOGGER.log(Level.ERROR, "Error processing keyrange request", e);
                            responseMessage = new SimpleKVMessage(StatusType.SERVER_STOPPED, null);
                        }
                        SimpleKVCommunication.sendMessage(responseMessage, output, LOGGER);

                    // Keyrange_read request
                    } else if (requestMessage.getStatus() == StatusType.KEYRANGE_READ){
                        try {
                            String response = server.keyrange_read();
                            responseMessage = new SimpleKVMessage(StatusType.KEYRANGE_READ_SUCCESS, response);
                            LOGGER.info("Processed keyrange read request and returned: " + requestMessage.getMsg());
                        } catch (Exception e) {
                            LOGGER.log(Level.ERROR, "Error processing keyrange read request", e);
                            responseMessage = new SimpleKVMessage(StatusType.SERVER_STOPPED, null);
                        }
                        SimpleKVCommunication.sendMessage(responseMessage, output, LOGGER);

                    // PUT/GET requests
                    } else {
                        System.out.println("HELLO... WE ARE DOING PUT/GET REQ for server " + server.getServerName());
                        System.out.println("requestMessage.getKey():" + requestMessage.getKey()); 

                        // Check if server is responsible
                        boolean isResponsible = false;
                        if (requestMessage.getStatus() == StatusType.GET) {
                            isResponsible = server.readMetadata.isServerResponsible(server.getServerName(), requestMessage.getKey());
                        } else {
                            isResponsible = server.metadata.isServerResponsible(server.getServerName(), requestMessage.getKey());
                        }
                        System.out.println("Is server responsible:" + isResponsible);

                        if (isResponsible == true) {
                            System.out.println("KEY IN RANGE CONFIRMED");
                            switch (requestMessage.getStatus()) {
                                case PUT:
                                    try {
                                        StatusType responseType;
                                        if (requestMessage.getValue() == null) { // DELETE operation
                                            System.out.println("DELETINNNNNNNNNNNNG...1:" + requestMessage.getKey());
                                            LOGGER.info("\n ...DELETE IN PROGRESS... \n");
                                            if (server.inStorage(requestMessage.getKey()) || server.inCache(requestMessage.getKey())) {
                                                server.putKV(requestMessage.getKey(), null);
                                                responseType = StatusType.DELETE_SUCCESS;
                                                LOGGER.info("Processed DELETE for key: " + requestMessage.getKey());
                                            } else {
                                                responseType = StatusType.DELETE_ERROR; // Key not found for deletion
                                                LOGGER.info("DELETE request failed for key: " + requestMessage.getKey() + ": key not found");
                                            }
                                        } else { // PUT operation
                                            boolean keyExists = server.inStorage(requestMessage.getKey()) || server.inCache(requestMessage.getKey());
                                            responseType = keyExists ? StatusType.PUT_UPDATE : StatusType.PUT_SUCCESS;
                                            server.putKV(requestMessage.getKey(), requestMessage.getValue());
                                            // responseType = server.inStorage(requestMessage.getKey()) ? StatusType.PUT_UPDATE : StatusType.PUT_SUCCESS;
                                        }
                                        responseMessage = new SimpleKVMessage(responseType, requestMessage.getKey(), requestMessage.getValue());
                                    } catch (Exception e) {
                                        LOGGER.log(Level.ERROR, "Error processing put request", e);
                                        responseMessage = new SimpleKVMessage(StatusType.PUT_ERROR, null, null);
                                    }
                                    break;
                                case GET:
                                    try {
                                        String response = server.getKV(requestMessage.getKey());
                                        StatusType responseType = (response != null) ? StatusType.GET_SUCCESS : StatusType.GET_ERROR;
                                        responseMessage = new SimpleKVMessage(responseType, requestMessage.getKey(), response);
                                        LOGGER.info("Processed GET request for key: " + requestMessage.getKey() + " with value: " + response);
                                    } catch (Exception e) {
                                        LOGGER.log(Level.ERROR, "Error processing get request", e);
                                        responseMessage = new SimpleKVMessage(StatusType.GET_ERROR, null, null);
                                    }
                                    break;
                                default:
                                    LOGGER.info("Received neither PUT or GET.");
                                    break;
                            }
                            if (responseMessage != null) { // Only send a response if responseMessage was set
                                SimpleKVCommunication.sendMessage(responseMessage, output, LOGGER);
                                LOGGER.info("responseString: " + responseMessage.getMsg());
                            }
                        } else {
                            // Server not responsible, respond with error and metadata
                            responseMessage = new SimpleKVMessage(StatusType.SERVER_NOT_RESPONSIBLE, null);
                            SimpleKVCommunication.sendMessage(responseMessage, output, LOGGER);
                        }
                    }
                } catch (SocketException se) {
                    LOGGER.info("Client disconnected.");
                    isOpen = false;
                    break; // Break out of the loop
                } catch (IOException ioe) {
                    LOGGER.log(Level.ERROR, "Error! Connection Lost!", ioe);
                    isOpen = false;
                    break; // Break out of the loop
                }
            }
            LOGGER.info("Client has closed the connection. Close listening client socket.");
        } catch (Exception e) {
            LOGGER.log(Level.ERROR, "Unexpected error in ClientHandler", e);
        } finally {
            try {
                if (clientSocket != null && !clientSocket.isClosed()) {
                    clientSocket.close();
                }

            } catch (IOException e) {
                LOGGER.log(Level.ERROR, "Error closing client socket", e);
            }
        }
    }

    public void sendShutdownMessage() {
        try {
            SimpleKVMessage shutdownMessage = new SimpleKVMessage(KVMessage.StatusType.SERVER_STOPPED, null);
            SimpleKVCommunication.sendMessage(shutdownMessage, output, LOGGER); // Implement this method based on your message sending logic
            LOGGER.info("Shutdown message sent to client");
        } catch (IOException e) {
            LOGGER.log(Level.ERROR, "Failed to send shutdown message to client", e);
        }
    }
}