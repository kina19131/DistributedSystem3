package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import client.KVCommInterface;
import shared.messages.KVMessage;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import client.KVStore;

public class KVClient implements IKVClient {

    private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "KVClient> ";
    private static final int MAX_KEY_BYTES = 20;
    private static final int MAX_VAL_BYTES = 122880;
	private BufferedReader stdin;
	private boolean stop = false;
	
	private String serverAddress;
	private int serverPort;

    private KVStore kvStore = null;

    @Override
    public void newConnection(String hostname, int port)  throws UnknownHostException, Exception {
        kvStore = new KVStore(hostname, port);
        kvStore.connect();
    }

    @Override
    public KVCommInterface getStore(){
        return kvStore;
    }

    private void handleCommand(String cmdLine) {
		cmdLine = cmdLine.trim(); 
		String[] tokens = cmdLine.split("\\s+");
		System.out.println("Received command: " + tokens[0]);

		if(tokens[0].equals("quit")) {	
			stop = true;
            disconnect();
			System.out.println(PROMPT + "Application exit!");
		
		} else if (tokens[0].equals("connect")){
			if(tokens.length == 3) {
				try{
					serverAddress = tokens[1];
					serverPort = Integer.parseInt(tokens[2]);
					newConnection(serverAddress, serverPort);
					System.out.println("Connected to: " + serverAddress + " " + serverPort);
				} catch(NumberFormatException nfe) {
					printError("No valid address. Port must be a number!");
					logger.info("Unable to parse argument <port>", nfe);
				} catch (UnknownHostException e) {
					printError("Unknown Host!");
					logger.info("Unknown Host!", e);
				} catch (Exception e) {
					printError("Could not establish connection!");
					logger.warn("Could not establish connection!", e);
				}
			} else {
				printError("Invalid number of parameters!");
			}
			
		} else if(tokens[0].equals("disconnect")) {
			disconnect();
            System.out.println(PROMPT + "Disconnected from the server!");

        } else if(tokens[0].equals("put")) {
            if(tokens.length >= 2) {                
                if(kvStore != null && kvStore.isRunning()) {
					// System.out.println("Attempting to put - step 1!");
					String key = tokens[1]; 
 
                    if (key.length() > 0 && key.length() <= MAX_KEY_BYTES ) {
						// System.out.println("Attempting to put - step 2!");
                        String value = null;
                        if (tokens.length > 2) {
                            value = parseValue(cmdLine);  
                        }
                        if ((value == null) || (value != null && value.length() <= MAX_VAL_BYTES)) {
                            try {
								// System.out.println("Attempting to put - step 3!");
                                KVMessage res = kvStore.put(key, value);
								System.out.println("Server response: " + res.getStatus());
                            } catch (Exception e) {
                                printError("Unable to perform put request!");
					            logger.error("Unable to perform put request!", e);
                            }
                        } else {
                            printError("Invalid value length!");
                        }
                    } else {
                        printError("Invalid key length!");
                    }
                } else {
                    printError("Not connected!");
                }
			} else {
				printError("Invalid number of parameters!");
			}

		} else if(tokens[0].equals("get")) {
            if(tokens.length == 2) {
				if(kvStore != null && kvStore.isRunning()){
					String key = tokens[1];
					System.out.println("Preparing to call kvStore.get with key: " + key);
                    try {
                        KVMessage res = kvStore.get(key);
						System.out.println("get RES: " + res);
						logger.info("Received response from server for GET request");
						System.out.println("Server response: " + res.getStatus());
                    } catch (Exception e) {
                        printError("Unable to perform get request!");
					    logger.error("Unable to perform get request!", e);
                    }
				} else {
					printError("Not connected!");
				}
			} else {
				printError("Invalid number of parameters!");
			}

		} else if(tokens[0].equals("logLevel")) {
			if(tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					System.out.println(PROMPT + "Possible log levels are:");
                    System.out.println(PROMPT 
                            + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
				} else {
					System.out.println(PROMPT + 
							"Log level changed to level " + level);
				}
			} else {
				printError("Invalid number of parameters!");
			}
			
		} else if(tokens[0].equals("help")) {
			printHelp();
        
		} else {
			printError("Unknown command");
			printHelp();
		}
	}

    private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}

    private void disconnect() {
		if (kvStore != null) {
			kvStore.disconnect();
			kvStore = null;
		}
	}

    private String parseValue(String cmd){
        int idxFirstSpace = cmd.indexOf(' ');
        int idxSecondSpace = cmd.indexOf(' ', idxFirstSpace + 1);
        return cmd.substring(idxSecondSpace + 1);
    }

    private String setLevel(String levelString) {
		
		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
        
        sb.append(PROMPT);
        sb.append("=======================================");
        sb.append("=======================================\n");
        
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t\t establishes a connection to a server\n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the servers\n");

        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t\t inserts a key-value pair to the server\n");
        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t\t retrieves the value for the key from the server\n");
        
        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel\n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF\n");
        
        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    public void run() {
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
			
			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI is not responding - Application terminated ");
			}
		}
	}

    public static void main(String[] args) {
    	try {
			new LogSetup("logs/client.log", Level.OFF);
			KVClient client = new KVClient();
			client.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
    }
}