package client; 

import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.SimpleKVMessage;
import shared.messages.SimpleKVCommunication;

public class KVCommunication implements Runnable {
    
    private Logger logger = Logger.getRootLogger();
    private boolean running;
    
    private Socket clientSocket;
    private OutputStream output;
    private InputStream input;
    
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

    /**
     * Initialize KVCommunication with address and port of KVServer
     * @param serverAddress the address of the KVServer
     * @param serverPort the port of the KVServer
     */
    public KVCommunication(String serverAddress, int serverPort) throws UnknownHostException, Exception {
        clientSocket = new Socket(serverAddress, serverPort);
        clientSocket.setSoTimeout(2000);
        setRunning(true);
        logger.info("Connection established.");
    }

    /**
     * Initializes and starts the client connection. 
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();

            while(isRunning()) {
                try {
                    KVMessage latestMsg = receiveMessage();
                    logger.info("Received message: " + latestMsg);
                    
                } catch (SocketTimeoutException soe) {
                    if(isRunning()) {
                        logger.error("Connection lost!");
                        try {
                            tearDownConnection();
                        } catch (IOException e) {
                            logger.error("Unable to close connection!");
                        }
                    }
                } catch (IOException ioe) {
                    if(isRunning()) {
                        logger.error("Connection lost!");
                        try {
                            tearDownConnection();
                        } catch (IOException e) {
                            logger.error("Unable to close connection!");
                        }
                    }
                }               
            }
        } catch (IOException ioe) {
            logger.error("Connection could not be established!");
        } finally {
            if(isRunning()) {
                closeConnection();
            }
        }
    }

    /**
     * For sending message to the KV server.
     */
    public SimpleKVMessage sendMessage(StatusType status, String key, String value) throws IOException {
        // Ensure the output stream is set up
        if (output == null) {
            throw new IOException("Output stream not initialized");
        }

        // Create SimpleKVMessage format
        SimpleKVMessage messageToSend = new SimpleKVMessage(status, key, value);
        
        // Send the message
        SimpleKVCommunication.sendMessage(messageToSend, output, logger);

        System.out.println("KVComm, Sent message: " + messageToSend); 
        return receiveMessage(); 

        // // Receive the response from the server and parse it
        // String response = receiveFormattedMessage();
        // return parseMessage(response);
    }

    /**
     * For receiving message from the KV server.
     */
    public SimpleKVMessage receiveMessage() throws IOException {
        // Ensure the input stream is set up
        if (input == null) {
            throw new IOException("Input stream not initialized");
        }
    
        // Read the response and parse it
        try {
            String response = SimpleKVCommunication.receiveMessage(input, logger);
            System.out.println("Received raw message: " + response); // MODIFIED: Added logging for raw received message
            return SimpleKVCommunication.parseMessage(response, logger);
        } catch (IOException e) {
            throw new IOException("Response not readable.");
        }
    }

    public void closeConnection() {
        logger.info("try to close connection ...");
        
        try {
            tearDownConnection();
        } catch (IOException ioe) {
            logger.error("Unable to close connection!");
        }
    }
    
    private void tearDownConnection() throws IOException {
        setRunning(false);
        logger.info("tearing down the connection ...");
        if (clientSocket != null) {
            input.close();
            output.close();
            clientSocket.close();
            clientSocket = null;
            logger.info("connection closed!");
        }
    }

    public boolean isRunning() {
        return running;
    }
    
    public void setRunning(boolean run) {
        running = run;
    }

    public void connect() throws IOException {
        output = clientSocket.getOutputStream();
        input = clientSocket.getInputStream();
    }
}