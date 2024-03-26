package app_kvServer;

import ecs.ECSNode;

import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.logging.Level;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;
import org.apache.log4j.Logger;
//import java.util.logging.Logger;


import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;

import shared.messages.KVMessage;
import shared.messages.SimpleKVMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.SimpleKVCommunication;


import app_kvServer.ClientHandler;

import java.math.BigInteger;


public class KVServer implements IKVServer {
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */

	 
	private String storagePath = ".";

	private String ecsHost = "localhost"; // ECSClient host
	private int ecsPort = 51000; // ECSClient listening port

	private ServerSocket serverSocket;
	private int port;
	private boolean running;
	private Set<ClientHandler> activeClientHandlers;
	private List<Thread> clientHandlerThreads;
	private ConcurrentHashMap<String, String> storage;
	private ConcurrentHashMap<String, String> cache;
	private Queue<String> fifoQueue;
	private ConcurrentHashMap<String, Integer> accessFrequency;
	private LinkedHashMap<String, String> lruCache;
	private PriorityBlockingQueue<String> lfuQueue;
	private int cacheSize;
	private IKVServer.CacheStrategy strategy;

	private static final String SECRET_TOKEN = "secret";

    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(KVServer.class.getName());
    private static final Logger LOG4J_LOGGER = Logger.getLogger(KVServer.class);

	private String serverName;
	private String[] keyRange = new String[2]; // ["lowHashValue", "highHashValue"]
	private String metadata; // Consider using a more complex structure if needed
	private String readMetadata;
	private volatile boolean writeLock = false;

	private List<ECSNode> successors;
	


		
	public KVServer(int port, int cacheSize, String strategy, String name) {

		this.serverName = "localhost:" + Integer.toString(port);
		
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = IKVServer.CacheStrategy.valueOf(strategy.toUpperCase());

		this.activeClientHandlers = Collections.synchronizedSet(new HashSet<ClientHandler>());
		this.clientHandlerThreads = Collections.synchronizedList(new ArrayList<Thread>());
		this.storage = new ConcurrentHashMap<String, String>();

		

		if (IKVServer.CacheStrategy.FIFO.equals(this.strategy)) {
			this.fifoQueue = new LinkedList<String>();
		} else if (IKVServer.CacheStrategy.LFU.equals(this.strategy)) {
			this.accessFrequency = new ConcurrentHashMap<String, Integer>();
			this.lfuQueue = new PriorityBlockingQueue<String>(cacheSize, new Comparator<String>() {
				public int compare(String key1, String key2) {
					int freqCompare = Integer.compare(accessFrequency.get(key1), accessFrequency.get(key2));
					return freqCompare != 0 ? freqCompare : key1.compareTo(key2);
				}
			});
		} else if (IKVServer.CacheStrategy.LRU.equals(this.strategy)) {
			initLRUCache();
		}

		if (cacheSize > 0) {
			this.cache = new ConcurrentHashMap<String, String>();
		}
		start();

		addShutdownHook(); 
	}

	private void addShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				System.out.println("Shutdown hook triggered (^C).");
				// Send shutdown message each client
				synchronized (activeClientHandlers) {
					for (ClientHandler handler : activeClientHandlers) {
						System.out.println("Sending shutdown message to client");
						handler.sendShutdownMessage();
					}
				}
				// Perform shutdown logic here
				stopServer(); // For example, safely stop the server
			}
		}));
	}

	public void sendMessageToECS(String message) {
		System.out.println("KVserver, sendMessageToECS - ECSClient alive? :" + isECSAvailable());
		try (Socket socket = new Socket(ecsHost, ecsPort);
			 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
			out.println(message);
		} catch (IOException e) {
			System.out.println("Could not send message to ECS at " + ecsHost + ":" + ecsPort + ". ECS might not be up yet.");
			// e.printStackTrace(); 
		}
	}


	private boolean isECSAvailable() {
		try (Socket socket = new Socket()) {
			// Try connecting to the ECSClient's port
			socket.connect(new InetSocketAddress("localhost", ecsPort), 1000); // timeout of 1000ms
			return true; // Connection successful, ECSClient is up
		} catch (IOException e) {
			System.out.println("ECS unavailable.");
			return false; // Connection failed, ECSClient might not be up
		}
	}

	
	public void updateMetadata(String newMetadata) {
		this.metadata = newMetadata;
		// TODO: Parse and apply the new metadata as needed
	}

	public void updateReadMetadata(String newMetadata) {
		this.readMetadata = newMetadata;
	}

	public String keyrange() {
		return metadata;
	}

	public String keyrange_read() {
		return readMetadata;
	}

	public void setWriteLock(boolean lock) {
		this.writeLock = lock;
	}

	// Use this method to check if write operations are allowed
	public boolean canWrite() {
		return !writeLock;
	}

	public void setKeyRange(String low, String high) {
		this.keyRange[0] = low;
		this.keyRange[1] = high;
	}

	public void start() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                KVServer.this.run();
            }
        }).start();
    }

	private void initLRUCache() {
		this.lruCache = new LinkedHashMap<String, String>(cacheSize, 0.75F, true) {
			protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
				return size() > KVServer.this.cacheSize;
			}
		};
	}

	public void setStoragePath(String storageDir) throws IOException {
		File dir = new File(storageDir);
		if (!dir.exists() || !dir.isDirectory()) {
			throw new IOException("The provided storage directory does not exist or is not a directory.");
		}
		this.storagePath = storageDir;
	}

	
	@Override
	public int getPort(){
		// TODO Auto-generated method stub
		if (serverSocket != null && !serverSocket.isClosed()){
			return serverSocket.getLocalPort();
		}
		return -1; 
	}

	@Override
	public String getHostname() {
    try {
        return InetAddress.getLocalHost().getHostAddress();
    	} 
	catch (UnknownHostException e) {
        LOGGER.info("Error getting host IP address");
        return null;
    	}
	}

	@Override 
	public CacheStrategy getCacheStrategy() {
		LOGGER.info("getCacheStrategy: " + this.strategy);
		try {
			return this.strategy;
		} catch (IllegalArgumentException | NullPointerException e) {
			LOGGER.info("Invalid or null cache strategy: " + this.strategy);
			return IKVServer.CacheStrategy.None;
		}
	}

	@Override
    public int getCacheSize(){
		// TODO Auto-generated method stub
		return this.cacheSize;
	}

	@Override
    public boolean inStorage(String key){
		// TODO Auto-generated method stub
		return storage.containsKey(key);
	}

	@Override
    public boolean inCache(String key){
		// TODO Auto-generated method stub
		return cache != null && cache.containsKey(key);
	}


	@Override
	public String getKV(String key) throws Exception {
		LOGGER.info("GETKV PROCESSING");
		String value = null; // Initialize value to null
		
		if (cache != null && inCache(key)) {
			value = cache.get(key);
			LOGGER.info("Cache hit for key: " + key);
		} 

		if (value == null && inStorage(key)){
			value = storage.get(key);
			LOGGER.info("Storage hit for key: " + key);
		}
		System.out.println("AHHH:" + value); 

		return value;
	}



	@Override
    public void putKV(String key, String value) throws Exception {
		try {
			if (value == null || "null".equals(value)) {
				// DELETE process
				storage.remove(key);
				LOGGER.info("Key removed from storage: " + key);
				// Add logic for removing from cache if applicable
			} else {
				// PUT process
				storage.put(key, value);
				replicateData(key, value); 
				LOGGER.info("Storage updated for key: " + key);
				// Add logic for updating cache and replication
			}
			saveDataToStorage(); // Assuming this is the correct method name
			
		} catch (Exception e) {
			LOGGER.info("Error while putting key: " + key + " with value: " + value);
			throw e;
		}
	}
	
	private void replicateData(String key, String value) {
		if (this.successors != null) {
			try {
				for (ECSNode successor : this.successors) {
					// Implement the logic to check if the key should be replicated to the successor
					// For example, check if the key's hash is within the successor's hash range
					SimpleKVCommunication.ServerToServer(StatusType.PUT, key, value, successor, LOG4J_LOGGER);
				}
			} catch (Exception e) {
				LOGGER.severe("Error while putting key: " + key + " with value: " + value);
				LOGGER.log(Level.SEVERE, e.getMessage(), e);
			}
		}
	}
	
	
	
	


	// UPDATING CACHE 
	private void updateCache(String key, String value) {
		LOGGER.info("UPDATING CACHE"); 	
		switch (getCacheStrategy()) {
			case FIFO:
				LOGGER.info("Update FIFO: Put Key: " + key + " with value:" + value); 	
				updateCacheFIFO(key, value);
				break;
			case LRU:
				if (lruCache != null) {
					updateCacheLRU(key, value);
				}
				LOGGER.info("LRU: Put Key: " + key + " with value:" + value); 	
				break;
			case LFU:
				updateCacheLFU(key, value);
				LOGGER.info("LFU: Put Key: " + key + " with value:" + value); 	
				break;
			case None:
				// No caching
				break;
		}
	}

	// FIFO: The oldest item is evicted when the cache is full.
	// LRU: The least recently used item is evicted. Your implementation keeps the most recently used items at the end of the cache map.
	// LFU: The least frequently used item is evicted. You use an accessFrequency map to track the access frequency of each key.
	// FIFO Update Cache Method
	private void updateCacheFIFO(String key, String value) {
		synchronized (fifoQueue) {
			if (fifoQueue.size() >= cacheSize && !cache.containsKey(key)) {
				String oldestKey = fifoQueue.poll();
				cache.remove(oldestKey);
			}
			if (!cache.containsKey(key)) {
				fifoQueue.offer(key);
			} else {
			}
			cache.put(key, value);
		}
	}

	// LRU Update Cache Method
	private void updateCacheLRU(String key, String value) {
		lruCache.put(key, value);
	}

	// LFU Update Cache Method
	private void updateCacheLFU(String key, String value) {
		if (cache.size() >= cacheSize && !cache.containsKey(key)) {
			String leastUsedKey = lfuQueue.poll();
			if (leastUsedKey != null) {
				cache.remove(leastUsedKey);
				accessFrequency.remove(leastUsedKey);
			}
		}
		cache.put(key, value);
		accessFrequency.put(key, accessFrequency.getOrDefault(key, 0) + 1);
		if (!lfuQueue.contains(key)) {
			lfuQueue.offer(key);
		}
	}


	private String findLeastFrequentKeyLFU() {
        String leastFrequentKey = null;
        int minFreq = Integer.MAX_VALUE;
        for (Map.Entry<String, Integer> entry : accessFrequency.entrySet()) {
            if (entry.getValue() < minFreq) {
                minFreq = entry.getValue();
                leastFrequentKey = entry.getKey();
            }
        }
        return leastFrequentKey;
    }

	@Override
	public void clearCache() {
		if (cache != null) {
			cache.clear();
		}
		if (fifoQueue != null) { // For FIFO
			fifoQueue.clear();
		}
		if (accessFrequency != null) { // For LFU
			accessFrequency.clear();
		}
		LOGGER.info("Cache cleared");
	}

	@Override
    public void clearStorage(){
		// TODO Auto-generated method stub
		storage.clear();
    	LOGGER.info("Storage cleared");
	}

	private boolean isRunning() {
        return this.running;
    }

	
	@Override
	public void run() {
		running = initializeServer();
		if (serverSocket == null || !running) {
			LOGGER.info("Server initialization failed. Server is not running.");
			return;
		}
		LOGGER.info("KV Server listening on port " + getPort());

		while (isRunning()) {
			try {
				Socket clientSocket = serverSocket.accept();
				LOGGER.info("Connection attempt from: " + clientSocket.getInetAddress());

				// Directly handle ECS command or initiate a client handler
				handleIncomingConnection(clientSocket);
			} catch (IOException e) {
				LOGGER.info("Error accepting client connection");
			}
		}
		saveDataToStorage();
	}
	
	
	private void handleIncomingConnection(Socket clientSocket) {
		try {
			int bufferSize = 2048;
			PushbackInputStream in = new PushbackInputStream(clientSocket.getInputStream(), bufferSize);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String command = reader.readLine();

			if (command != null){

				System.out.println("RECIEVED COMMAND: " + command);
				
				byte[] bytesToUnread = (command + "\r\n").getBytes("UTF-8"); // Convert the command back to bytes and push them back to the stream
				in.unread(bytesToUnread);
		
				if (command.startsWith(SECRET_TOKEN)) {
					handleECSorServerCommand(command);
				} 
				else {
					// Handle non-ECS connections, aka KVclients
					LOGGER.info("Handling client connection");
					// System.out.println("KVServer, keyRange: " + keyRange); 
					System.out.println("KVServer, keyRange: " + Arrays.toString(keyRange));
					System.out.println("KVServer, server: " + this); 
					ClientHandler handler = new ClientHandler(clientSocket, this, keyRange, in); 
					activeClientHandlers.add(handler);
					Thread handlerThread = new Thread(handler);
					clientHandlerThreads.add(handlerThread);
					handlerThread.start();
				}
			}
		} catch (Exception e) {
			LOGGER.info( "Error handling incoming connection");
		}
	}
	
	private void updateSuccessorList(List<ECSNode> newSuccessors) {
		this.successors = newSuccessors;
		LOGGER.info("Updated successors list: " + this.successors);
	}
	
	
	private void handleECSorServerCommand(String command) {
		String[] parts = command.split(" ", 4);
		switch (parts[1]) {
			case "SET_CONFIG":
				setKeyRange(parts[2], parts[3]);
				LOGGER.info("Key range updated: " + Arrays.toString(keyRange));
				break;
			case "SET_METADATA":
				updateMetadata(parts[2]);
				LOGGER.info("Metadata updated.");
				break;
			case "SET_READ_METADATA":
				updateReadMetadata(parts[2]);
				LOGGER.info("Metadata updated.");
				break;
			case "ECS_REQUEST_STORAGE_HANDOFF":
				System.out.println("KVServer, ECS_REQ_STG_HANDOFF:"+command); 
				// Trigger storage handoff procedure
				handOffStorageToECS("NEW_SERVER");
				break;
			case "SET_WRITE_LOCK":
				setWriteLock(Boolean.parseBoolean(parts[2]));
				LOGGER.info("Write lock set to: " + parts[2]);
				break;

			case "UPDATE_SUCCESSORS":
				System.out.println("KVServer, UPDATE_SUCCESSORS:"+command); 
				List<ECSNode> newSuccessors = deserializeSuccessors(parts[2]);
				updateSuccessorList(newSuccessors);
				break;
					
			case "PUT":
				try {
					if (parts.length > 3){
						putKV(parts[2], parts[3]);
					} else {
						putKV(parts[2], null);
					}
				} catch (Exception e) {
					LOGGER.info("Unable to perform PUT request from ECS");
				}
				break;
			default:
				LOGGER.info("Received unknown ECS command: " + command);
				break;
		}
	}

	private List<ECSNode> deserializeSuccessors(String serializedData) {
		List<ECSNode> successors = new ArrayList<>();
		String[] parts = serializedData.split(",");
		for (String part : parts) {
			String[] nodeInfo = part.split(":");
			if (nodeInfo.length == 4) {
				String host = nodeInfo[0];
				int port = Integer.parseInt(nodeInfo[1]);
				String lower = nodeInfo[2];
				String upper = nodeInfo[3];
	
				// The node name can be derived from the host and port or provided as needed
				String nodeName = host + ":" + port;
				String cacheStrategy = "LRU"; // Default cache strategy
				int cacheSize = 1000; // Default cache size
	
				ECSNode node = new ECSNode(nodeName, host, port, cacheStrategy, cacheSize, lower, upper);
				successors.add(node);
			}
		}
		return successors;
	}
	
	
	


	private void loadDataFromStorage() {
		String fileName = "kvstorage_" + serverName + ".txt";
		String filePath = storagePath + File.separator + fileName;
		File file = new File(filePath);
	
		try {
			if (!file.exists()) {
				// If the file doesn't exist, create an empty file
				boolean created = file.createNewFile();
				if (created) {
					LOGGER.info("Created new " + filePath + " file");
				} else {
					LOGGER.info("Failed to create " + filePath + " file");
				}
			}
	
			try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
				String line;
				while ((line = reader.readLine()) != null) {
					String[] parts = line.split(",");
					if (parts.length == 2) {
						storage.put(parts[0], parts[1]);
					}
				}
				LOGGER.info("Loaded data from " + filePath + " file");
			} catch (IOException e) {
				LOGGER.info("Error loading data from " + filePath + " file");
			}
		} catch (IOException e) {
			LOGGER.info("Error creating " + filePath + " file");
		}
	}


	public void handOffStorageToECS(String occasion) {
		System.out.println("KVServer, handOffStorageToECS");
	
		StringBuilder sb = new StringBuilder();
		// Replace lambda expression with traditional for-loop for Java 7 compatibility
		for (Map.Entry<String, String> entry : storage.entrySet()) {
			sb.append(entry.getKey()).append("=").append(entry.getValue()).append(";");
		}
	
		// Remove the last semicolon to avoid an empty entry when splitting
		if (sb.length() > 0) {
			sb.setLength(sb.length() - 1);
		}
	
		String serializedStorage = sb.toString();
	
		// Determine the message prefix based on the occasion
		String messagePrefix = (occasion.equals("DEAD_SERVER")) ? "STORAGE_HANDOFF " : "ECS_STORAGE_HANDOFF ";
	
		// Send the serialized data or a notification of no data to ECS
		Socket ecsSocket = null;
		PrintWriter out = null;
		try {
			ecsSocket = new Socket(ecsHost, ecsPort);
			out = new PrintWriter(ecsSocket.getOutputStream(), true);
			// If there's data to send, include it; otherwise, just notify ECS of the server status
			String messageToSend = sb.length() > 0 ? messagePrefix + serverName + " " + serializedStorage : messagePrefix + serverName;
			out.println(messageToSend);
			System.out.println("Sent: " + messageToSend);
		} catch (IOException e) {
			LOGGER.info("Error sending storage data to ECS for " + occasion);
		} finally {
			if (out != null) {
				out.close();
			}
			if (ecsSocket != null) {
				try {
					ecsSocket.close();
				} catch (IOException e) {
					LOGGER.info("Error closing socket");
				}
			}
		}
	}
	

	public void stopServer() {
		running = false;
		try {
			if (serverSocket != null && !serverSocket.isClosed()) {
				sendMessageToECS("DYING_MSG " + serverName);
				handOffStorageToECS("DEAD_SERVER");
				saveDataToStorage();
				System.out.println("stopping server, handed off storaget to ECS");
				serverSocket.close();
			}
		} catch (IOException e) {
			LOGGER.info("Error closing server socket");
		}
	}

	private boolean initializeServer() {
        if (serverSocket == null) {
            try {
                serverSocket = new ServerSocket(port);
				sendMessageToECS("ALIVE " + serverName + " " + null); 
                return true;
            } catch (IOException e) {
                LOGGER.info("Error! Cannot open server socket:");
                return false; // server socket cannot be opened
            }
        }
        return true;
    }


	private void saveDataToStorage() {
		String fileName = "kvstorage_" + serverName + ".txt";
		String filePath = storagePath + File.separator + fileName;

		try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
			for (Entry<String, String> entry : storage.entrySet()) {
				writer.write(entry.getKey() + "," + entry.getValue());
				writer.newLine();
			}
			LOGGER.info("Storage data saved to file");
		} catch (IOException e) {
			LOGGER.info("Error saving data to storage file");
		}
	}
	
	
	@Override
	public void close() {
		try {
			running = false;

			// Close the server socket
			if (serverSocket != null && !serverSocket.isClosed()) {
				serverSocket.close();
			}

			// Wait for client handler threads to complete
			for (Thread thread : clientHandlerThreads) {
				try {
					thread.join(); // Wait for the thread to finish
				} catch (InterruptedException e) {
					LOGGER.info("Error waiting for client handler thread to complete: " + e.getMessage());
				}
			}
			saveDataToStorage();
		} catch (IOException e) {
			LOGGER.info("Error while closing the server: " + e.getMessage());
			e.printStackTrace();
		}
	}

	@Override
    public void kill(){
		running = false; 
		try{
			saveDataToStorage();
			if(serverSocket != null && !serverSocket.isClosed()){
				serverSocket.close(); 
			}
		// Immediately terminate any ongoing processing
        // This might involve interrupting active threads or shutting down a thread pool

		} catch (IOException e) {
			e.printStackTrace();
		}
		LOGGER.info("Server Socket Closed");
	}



	public static void main(String[] args) {
		int port = 50000; // Default port
		String name = null; 
		int cacheSize = 10; // Example default cache size
		String strategy = "FIFO"; // Default strategy
		String address = "localhost"; // Default address
		String logFilePath = System.getProperty("user.dir") + File.separator+ "src" + File.separator + "logger"+ File.separator + "server.log"; // Default log file path
		Level logLevel = Level.ALL; // Default log level
		String storageDir = System.getProperty("user.dir") + File.separator+ "src" + File.separator + "logger"; // Default storage directory

		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
				case "-n": 
					if (i + 1 < args.length) name = args[++i]; 
					break; 
				case "-p":
					if (i + 1 < args.length) port = Integer.parseInt(args[++i]);
					break;
				case "-a":
					if (i + 1 < args.length) address = args[++i];
					break;
				case "-d":
					if (i + 1 < args.length) storageDir = args[++i];
					break;
				case "-l":
					if (i + 1 < args.length) logFilePath = args[++i];
					break;
				case "-ll":
					if (i + 1 < args.length) logLevel = Level.parse(args[++i]);
					break;
				case "-h":
					// Display help information
					System.out.println("Usage: java -jar KVServer.jar [-n name] [-p port] [-a address] [-d storageDir] [-l logFilePath] [-ll logLevel]");
					System.exit(0);
					break;
			}
		}

		try {
			FileHandler fileHandler = new FileHandler(logFilePath, true);
			fileHandler.setFormatter(new SimpleFormatter());
			// LOGGER.addHandler(fileHandler);
			// LOGGER.setLevel(logLevel);
		} catch (IOException e) {
			System.err.println("Error setting up logger: " + e.getMessage());
			System.exit(1);
		}

		// Initialize and start the server
		KVServer server = new KVServer(port, cacheSize, strategy, name);
		try {
			server.setStoragePath(storageDir);
		} catch (IOException e) {
			System.err.println("Error setting storage directory: " + e.getMessage());
			System.exit(1);
		}
	}

}