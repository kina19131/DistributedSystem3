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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;

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

    private String hashKey(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(key.getBytes());
            BigInteger no = new BigInteger(1, messageDigest);
            while (no.toString(16).length() < 32) {
                no = new BigInteger("0" + no.toString(16), 16);
            }
            return no.toString(16);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
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
                    System.out.println("msg:" + msg);
                    System.out.println("... REACHED CLIENT HANDLER ... 3");
                    SimpleKVMessage requestMessage = SimpleKVCommunication.parseMessage(msg, LOGGER);

                    String keyHash = hashKey(requestMessage.getKey());
                    System.out.println("ClientHandler, Client keyHash: " + keyHash);

                    if (server.isKeyInRange(keyHash)) {
                        System.out.println("KEY IN RANGE CONFIMED");
                        switch (requestMessage.getStatus()) {
                            case PUT:
                                try {
                                    StatusType responseType;
                                    if (requestMessage.getValue() == null) { // DELETE operation
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
                                        server.putKV(requestMessage.getKey(), requestMessage.getValue());
                                        responseType = server.inStorage(requestMessage.getKey()) ? StatusType.PUT_UPDATE : StatusType.PUT_SUCCESS;
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

                            case KEYRANGE:
                                try {
                                    String response = server.keyrange();
                                    responseMessage = new SimpleKVMessage(StatusType.KEYRANGE_SUCCESS, response);
                                    LOGGER.info("Processed keyrange request and returned: " + requestMessage.getMsg());
                                } catch (Exception e) {
                                    LOGGER.log(Level.ERROR, "Error processing get request", e);
                                    responseMessage = new SimpleKVMessage(StatusType.SERVER_STOPPED, null);
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
                        if (requestMessage.getStatus() == StatusType.KEYRANGE){
                            try {
                                String response = server.keyrange();
                                responseMessage = new SimpleKVMessage(StatusType.KEYRANGE_SUCCESS, response);
                                LOGGER.info("Processed keyrange request and returned: " + requestMessage.getMsg());
                            } catch (Exception e) {
                                LOGGER.log(Level.ERROR, "Error processing get request", e);
                                responseMessage = new SimpleKVMessage(StatusType.SERVER_STOPPED, null);
                            }
                        }
                        responseMessage = new SimpleKVMessage(StatusType.SERVER_NOT_RESPONSIBLE, null);
                        SimpleKVCommunication.sendMessage(responseMessage, output, LOGGER);
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


    
}