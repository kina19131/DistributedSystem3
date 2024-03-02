package app_kvServer;

import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;


import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;

import shared.messages.KVMessage;
import shared.messages.SimpleKVMessage;

import app_kvServer.ClientHandler;



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
	private static final Logger LOGGER = Logger.getLogger(KVServer.class.getName());

	private static final String ECS_SECRET_TOKEN = "secret";

	private String serverName;
	private String[] keyRange = new String[2]; // ["lowHashValue", "highHashValue"]
	private String metadata; // Consider using a more complex structure if needed
	private volatile boolean writeLock = false;


		
	public KVServer(int port, int cacheSize, String strategy, String name) {

		this.serverName = name;
		
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
		try (Socket socket = new Socket(ecsHost, ecsPort);
			 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
			out.println(message);
		} catch (IOException e) {
			System.out.println("Could not send message to ECS at " + ecsHost + ":" + ecsPort + ". ECS might not be up yet.");
			// e.printStackTrace(); 
		}
	}
	
	
	
	public void updateMetadata(String newMetadata) {
		this.metadata = newMetadata;
		// TODO: Parse and apply the new metadata as needed
	}

	public String keyrange() {
		return metadata;
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
        LOGGER.log(Level.SEVERE, "Error getting host IP address", e);
        return null;
    	}
	}

	@Override 
	public CacheStrategy getCacheStrategy() {
		LOGGER.info("getCacheStrategy: " + this.strategy);
		try {
			return this.strategy;
		} catch (IllegalArgumentException | NullPointerException e) {
			LOGGER.warning("Invalid or null cache strategy: " + this.strategy);
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
			LOGGER.fine("Cache hit for key: " + key);
		} 

		if (value == null && inStorage(key)){
			value = storage.get(key);
			LOGGER.fine("Storage hit for key: " + key);
		}
		return value;
	}



	@Override
    public void putKV(String key, String value) throws Exception{
		// TODO Auto-generated method stub
		// LOGGER.info("Attempting to put key: " + key + ", value: " + value);
		try{
			if (!canWrite()) {
				throw new IllegalStateException("Server is currently under write lock.");
			}

			if (value == null || "null".equals(value)) {
				if (storage.containsKey(key)) {
					storage.remove(key);
					LOGGER.info("Key removed from storage: " + key);
				}
				if (cache != null && cache.containsKey(key)) {
					cache.remove(key);
					if (strategy == CacheStrategy.LFU && accessFrequency.containsKey(key)) {
						accessFrequency.remove(key);
						lfuQueue.remove(key);
					}
					LOGGER.info("Key removed from cache: " + key);
				}
				return; 
			}

			storage.put(key, value); // if key already exists, get new val, will be updated 
									// if key not available, will be put in. 
			LOGGER.info("Storage updated for key: " + key);
			if (cache != null) {
				updateCache(key, value);  
				LOGGER.info("Cache updated for key: " + key);
			}
			saveDataToStorage(); 
		} catch (Exception e){
			LOGGER.severe("Error while putting key: " + key+ " with value: "+ value); 
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
			throw e; 
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
			LOGGER.severe("Server initialization failed. Server is not running.");
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
				LOGGER.log(Level.SEVERE, "Error accepting client connection", e);
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

				LOGGER.info("RECIEVED COMMAND: " + command);
				
				byte[] bytesToUnread = (command + "\r\n").getBytes("UTF-8"); // Convert the command back to bytes and push them back to the stream
				in.unread(bytesToUnread);
		
				if (command.startsWith(ECS_SECRET_TOKEN)) {
					handleECSCommand(command);
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
			LOGGER.log(Level.SEVERE, "Error handling incoming connection", e);
		}
	}
	
	
	

	private void handleECSCommand(String command) {
		String[] parts = command.split(" ");
		if (parts.length == 4 && "SET_CONFIG".equals(parts[1])) {
			setKeyRange(parts[2], parts[3]);
			LOGGER.info("Configuration updated: lowerHash=" + parts[2] + ", higherHash=" + parts[3]);
			// Acknowledge the ECS if needed
		} if (parts.length == 3 && "SET_METADATA".equals(parts[1])) {
			updateMetadata(parts[2]);
			LOGGER.info("Metadata updated: " + parts[2]);
			// Acknowledge the ECS if needed
		} else {
			LOGGER.warning("Invalid ECS command received: " + command);
		}
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
					LOGGER.warning("Failed to create " + filePath + " file");
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
				LOGGER.log(Level.SEVERE, "Error loading data from " + filePath + " file", e);
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "Error creating " + filePath + " file", e);
		}
	}

	// Method to serialize the storage map and send it to ECS
    private void handOffStorageToECS() {
		
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : storage.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append(";");
        }
        // Remove the last semicolon to avoid an empty entry when splitting
        if (sb.length() > 0) sb.setLength(sb.length() - 1);

        String serializedStorage = sb.toString();

        // Send the serialized data
        try (Socket ecsSocket = new Socket(ecsHost, ecsPort); // Replace ECS_HOST and ECS_PORT with actual values
             PrintWriter out = new PrintWriter(ecsSocket.getOutputStream(), true)) {
            out.println("STORAGE_HANDOFF " + serverName + " " + serializedStorage);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error sending storage data to ECS", e);
        }
    }

	/* RECEIVE redistributed data FROM ECSClient */




	public void stopServer() {
		running = false;
		try {
			if (serverSocket != null && !serverSocket.isClosed()) {
				sendMessageToECS("DYING_MSG " + serverName);
				handOffStorageToECS();
				saveDataToStorage();
				System.out.println("stopping server, handed off storaget to ECS");
				serverSocket.close();
			}
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "Error closing server socket", e);
		}
	}

	private boolean initializeServer() {
        if (serverSocket == null) {
            try {
                serverSocket = new ServerSocket(port);
				sendMessageToECS("ALIVE " + serverName); // NEW 
                return true;
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Error! Cannot open server socket:", e);
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
			LOGGER.log(Level.SEVERE, "Error saving data to storage file", e);
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
					LOGGER.warning("Error waiting for client handler thread to complete: " + e.getMessage());
				}
			}
			saveDataToStorage();
		} catch (IOException e) {
			LOGGER.warning("Error while closing the server: " + e.getMessage());
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
		String name = "ServerX"; 
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
					System.out.println("Usage: java -jar KVServer.jar [-p port] [-a address] [-d storageDir] [-l logFilePath] [-ll logLevel]");
					System.exit(0);
					break;
			}
		}

		try {
			FileHandler fileHandler = new FileHandler(logFilePath, true);
			fileHandler.setFormatter(new SimpleFormatter());
			LOGGER.addHandler(fileHandler);
			LOGGER.setLevel(logLevel);
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