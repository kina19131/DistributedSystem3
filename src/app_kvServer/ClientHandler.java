package app_kvServer;

import shared.messages.SimpleKVMessage;
import shared.messages.KVMessage.StatusType;

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

public class ClientHandler implements Runnable {
    private Socket clientSocket;
    private KVServer server; 
    private boolean isOpen;
    private InputStream input;
    private OutputStream output;

    private static final Logger LOGGER = Logger.getRootLogger();


    public ClientHandler(Socket socket, KVServer server) {
        this.clientSocket = socket;
        this.server = server; 
        this.isOpen = true;
    }

    @Override
    public void run() {
        try {
            output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();

            while (isOpen) {

                try {
                    SimpleKVMessage responseMessage = null;
                    
                    String msg = SimpleKVCommunication.receiveMessage(input, LOGGER);
                    SimpleKVMessage requestMessage = SimpleKVCommunication.parseMessage(msg, LOGGER);
                    
                    switch(requestMessage.getStatus()){
                        case PUT:
                            try {
                                StatusType responseType; 
                                boolean inStorage = server.inStorage(requestMessage.getKey()); 
                                boolean inCache = server.inCache(requestMessage.getKey()); 

                                if ("null".equals(requestMessage.getValue()) || requestMessage.getValue().isEmpty()){

                                    LOGGER.info("\n ...DELETE IN PROGRESS... \n");
                                    if (inStorage || inCache){ // STORED IN STORAGE 
                                        server.putKV(requestMessage.getKey(), null);
                                        responseType = StatusType.DELETE_SUCCESS;
                                        LOGGER.info("Processed DELETE for key: " + requestMessage.getKey());
                                    }
                                    else{
                                        responseType = StatusType.DELETE_ERROR; // Key not found for deletion
                                        LOGGER.info("DELETE request failed for key: " + requestMessage.getKey() + ": key not found");
                                    }
                                }

                                else{ // GOT VALUE (UPDATE / PUT)
                                    server.putKV(requestMessage.getKey(), requestMessage.getValue());
                                    boolean isUpdate = inStorage || inCache; 
                                    responseType = isUpdate ? StatusType.PUT_UPDATE : StatusType.PUT_SUCCESS;
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
                    if(responseMessage != null){ // Only send a response if responseMessage was set
                        SimpleKVCommunication.sendMessage(responseMessage, output, LOGGER);
                        LOGGER.info("responseString: "+ responseMessage.getMsg());
                    }
                } catch (SocketException se) {
                                LOGGER.info("Client disconnected."); 
                                isOpen = false;
                } catch (IOException ioe) {
				    LOGGER.log(Level.ERROR, "Error! Connection Lost!", ioe);
				    isOpen = false;
				}
            }
            LOGGER.info("Client has closed the connection. Close listening client socket.");
        } catch (IOException e) {
            LOGGER.log(Level.ERROR, "Error in ClientHandler", e);
        } finally {
            try {
                if (!clientSocket.isClosed()) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                LOGGER.log(Level.ERROR, "Error closing client socket", e);
            }
        }
    }
    
}