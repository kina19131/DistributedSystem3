package app_kvECS;

import java.net.ServerSocket;

import java.util.List;
import java.util.ArrayList;

import shared.messages.KVMessage.StatusType;
import shared.messages.SimpleKVCommunication;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Collection;
import java.util.ArrayList;
import java.util.TreeMap; // Add import statement for TreeMap

// import java.util.logging.Logger;

import org.apache.log4j.Logger;
import java.util.logging.Level;


import java.net.Socket;
import java.io.*;

import ecs.ConsistentHashing;
import ecs.ECSNode;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;

import ecs.IECSNode;

import shared.messages.KVMessage;
import shared.messages.SimpleKVMessage;

public class ECSClient implements IECSClient {
    private int ecsPort;
    private Map<String, IECSNode> nodes = new HashMap<>();  //track the KVServer nodes
    private HashRing ecsHashRing = new HashRing();
    private String lowHashRange;
    private String highHashRange;
    private String nodesMetadata;
    private String nodesReadMetadata;
    // private static final Logger LOGGER = Logger.getLogger(ECSClient.class);


    private static final String ECS_SECRET_TOKEN = "secret";
    private TreeMap<BigInteger, IECSNode> hashRing = new TreeMap<>();

    private Map<String, String[]> nodeNameToHashRange = new HashMap<>();

    private static final Logger LOGGER = Logger.getLogger(ECSClient.class);
    // private static final Logger LOGGER = Logger.getLogger(ECSClient.class);

    private final Map<String, Long> lastHeartbeat = new ConcurrentHashMap<>();
    private long heartbeatInterval = 2000;
    private int heartbeatCheckInterval = 2000;

    private boolean isRunning;
    private ServerSocket serverSocket;


    public String[] getHashRangeForNode(String nodeName) {
        return nodeNameToHashRange.get(nodeName);
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        return new HashMap<String, IECSNode>(nodes); 
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        return null;
    }

    public IECSNode getNodeByName(String nodeName) {
        return nodes.get(nodeName);
    }
    

    private String getServerHash(String ip, int port) {
        return getMD5Hash(ip + ":" + port);
    }

    public ECSClient(int ecsPort){
        this.ecsPort = ecsPort; 
    }

    
    public void startListening() {
        isRunning = true;
        try (ServerSocket serverSocket = new ServerSocket(ecsPort)) {
            LOGGER.info("ECSClient listening on port " + ecsPort);

            while (isRunning) { // Continuously accept new connections
                Socket clientSocket = null;
                BufferedReader in = null;
                try {
                    clientSocket = serverSocket.accept();
                    in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream())); 
                    String inputLine = in.readLine();
                    // System.out.println("ECSClient:" + inputLine);

                    // Heartbeat message
                    if (inputLine != null && inputLine.startsWith("HEARTBEAT")) {
                        String serverName = inputLine.split(" ")[1];
                        lastHeartbeat.put(serverName, System.currentTimeMillis());
                        // System.out.println("Received heartbeat from " + serverName);
                    }

                    // New Server became available, adding it 
                    if (inputLine != null && inputLine.startsWith("ALIVE")) {
                        String[] parts = inputLine.split(" ", 3); // Split into at most 3 parts
                        String[] nodeNames = new String[1];
                        nodeNames[0] = parts[1];

                        System.out.println("SERVER SENT ALIVE MSG, Adding node... " + parts[1]);
                        setWriteLockAllNodes(true);
                        Collection<IECSNode> addedNodes = addNodes(1, "FIFO", 1024, nodeNames);
                        setWriteLockAllNodes(false);
                        System.out.println("Added nodes: " + addedNodes.size());
                    } 

                    // Recieved Data from Server - needed to rebalance (migrate) data
                    if (inputLine != null && inputLine.startsWith("ECS_STORAGE_HANDOFF")) {
                        String[] parts = inputLine.split(" ", 3); // Split into at most 3 parts

                        // if less than 3, there was no data to be handed over 

                        if (parts.length == 3){
                            System.out.println("ECSClient, Added node - handling data redistribution");
                            System.out.println("Received:" + inputLine);
                            String server = parts[1]; 
                            setWriteLockAllNodes(true);
                            processStorageHandoff(server, parts[2]);
                            setWriteLockAllNodes(false);
                        }
                    }
                    
                    // Dead Server has let know its dying, taking over the storage
                    if (inputLine != null && inputLine.startsWith("STORAGE_HANDOFF")) {
                        System.out.println("ECSClient, DeadServer");
                        String[] parts = inputLine.split(" ", 3); // Split into at most 3 parts
                    
                        // Even if there's no data to hand off (parts.length < 3), proceed to remove the node.
                        String dead_server = parts[1];
                        setWriteLockAllNodes(true);

                        Collection<String> nodeNamesToRemove = new ArrayList<>();
                        nodeNamesToRemove.add(dead_server); // Add the dead server to the collection
                        boolean removeSuccess = removeNodes(nodeNamesToRemove); // Call the removeNodes method
                    
                        if (removeSuccess) {
                            System.out.println("Node removed successfully: " + dead_server);
                            // Process storage handoff if there's data
                            if (parts.length == 3) {
                                processStorageHandoff(dead_server, parts[2]);
                            }
                        } else {
                            System.out.println("Failed to remove node: " + dead_server);
                        }

                        setWriteLockAllNodes(false);
                    
                        // After processing, check if there are no nodes left
                        // if (nodes.isEmpty()) {
                        //     safelyShutdownECSClient();
                        // } else {
                        //     System.out.println("There are still alive nodes. ECS will not shutdown.");
                        // }
                    }
                    
                    
                } catch (IOException e) {
                    // LOGGER.log(Level.SEVERE, "Error processing connection", e);
                    LOGGER.error("Error processing connection", e);
                } finally {
                    if (in != null) {
                        try {
                            in.close();
                        } catch (IOException e) {
                            // LOGGER.log(Level.SEVERE, "Error closing BufferedReader", e);
                            LOGGER.error("Error closing BufferedReader", e);

                        }
                    }
                    if (clientSocket != null && !clientSocket.isClosed()) {
                        try {
                            clientSocket.close();
                        } catch (IOException e) {
                            // LOGGER.log(Level.SEVERE, "Error closing Socket", e);
                            LOGGER.error("Error closing Socket", e);
                        }
                    }
                }
            }
        } catch (IOException e) {
            // LOGGER.log(Level.SEVERE, "Could not listen on port " + ecsPort, e);
            LOGGER.error("Could not listen on port ", e);
        } finally {
            if (serverSocket != null && !serverSocket.isClosed()) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    // LOGGER.log(Level.SEVERE, "Error closing the server socket", e);
                    LOGGER.error("Error closing the server socket", e);
                }
            }
        }
    }

    public void monitorHeartbeats() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("STARTING HEARTBEAT MONITOR");
                while (true) {
                    long currentTime = System.currentTimeMillis();
                    Collection<String> nodeNamesToRemove = new ArrayList<>();
                    for (Map.Entry<String, Long> entry : lastHeartbeat.entrySet()) {
                        if (currentTime - entry.getValue() > heartbeatInterval) {
                            // Handle the server node considered as down
                            String nodeName = entry.getKey();
                            IECSNode nodeToRemove = nodes.get(nodeName); 
                            if (nodeToRemove != null) {
                                System.out.println("Node " + nodeName + " is considered down.");
                                nodeNamesToRemove.add(nodeName); // Add the dead server to the collection
                            }
                        }
                    }
    
                    // Remove the "dead" nodes from active nodes, rebalance, etc.
                    if (!nodeNamesToRemove.isEmpty()) {
                        setWriteLockAllNodes(true);
                        boolean removeSuccess = removeNodes(nodeNamesToRemove);
                        if (removeSuccess) {
                            System.out.println("Nodes removed successfully.");
                        } else {
                            System.out.println("Failed to remove nodes.");
                        }
                        setWriteLockAllNodes(false);
                    }
    
                    try {
                        Thread.sleep(heartbeatCheckInterval);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }).start();
    }

    private void safelyShutdownECSClient() {
        System.out.println("No nodes are alive. Proceeding to stop services and shutdown ECS.");
    
        // Shutdown ECS
        boolean stopSuccess = stop(); // Attempt to stop all services
        boolean shutdownSuccess = shutdown(); // Attempt to shutdown ECS
        System.out.println("ECS shut down: " + shutdownSuccess);
    
        // Close server socket and release resources before exiting
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            LOGGER.error("Error closing the server socket", e);
        } finally {
            System.exit(0); // Safely exit after ensuring all resources are cleaned up
        }
    }

    public void close() {
        System.out.println("No nodes are alive. Proceeding to stop services and shutdown ECS.");
    
        // Shutdown ECS
        boolean stopSuccess = stop(); // Attempt to stop all services
        boolean shutdownSuccess = shutdown(); // Attempt to shutdown ECS
        System.out.println("ECS shut down: " + shutdownSuccess);
    
        // Close server socket and release resources before exiting
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            LOGGER.error("Error closing the server socket", e);
        } 
    }
    
    
    public void stopListening() {
        isRunning = false;
        // No need to close the serverSocket here since it's handled in the finally block of startListening method
    }


    /* Handle Handed off Storage */
    private void processStorageHandoff(String serverName, String serializedData) {
        Map<String, String> dataToRedistribute = deserializeStorage(serializedData);
        // Now you have the deserialized storage map from serverName, process it as needed
        LOGGER.info("Received storage handoff from " + serverName + ": " + dataToRedistribute);
        redistributeData(serverName, dataToRedistribute);
    }

    private Map<String, String> deserializeStorage(String serializedData) {
        Map<String, String> storage = new HashMap<>();
        String[] entries = serializedData.split(";");
        for (String entry : entries) {
            System.out.println(entry);
            String[] keyValue = entry.split("=");
            if (keyValue.length == 2) {
                storage.put(keyValue[0], keyValue[1]);
            }
        }
        return storage;
    }


    private void fetchServerData(IECSNode node) {
        String serializedData = ""; // Initialize an empty string to hold the serialized data
        try (Socket socket = new Socket(node.getNodeHost(), node.getNodePort());
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // Send a command to the server asking for its data
            out.println(ECS_SECRET_TOKEN + " ECS_REQUEST_STORAGE_HANDOFF");


        } catch (IOException e) {
            //LOGGER.log(Level.SEVERE, "Error fetching server data from node: " + node.getNodeName(), e);
            LOGGER.error("Error fetching server data from node", e);
        }

    }

    private void redistributeData(String oldServer, Map<String, String> dataToRedistribute) {
        System.out.println("ECSClient, OLDSERVER!!! : " + getNodeByName(oldServer));
        if (getNodeByName(oldServer) == null){ // DeadServer 
            if (!dataToRedistribute.isEmpty()){
                for (Map.Entry<String, String> entry : dataToRedistribute.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
            
                    String keyHash = ConsistentHashing.getKeyHash(key);
                    IECSNode targetNode = findNodeForKey(keyHash);
            
                    if (targetNode != null) {
                        SimpleKVCommunication.sendToServer(StatusType.PUT, key, value, targetNode, LOGGER);
                    } else {
                        //LOGGER.log(Level.SEVERE, "No target node found for key: " + key);
                        LOGGER.error("No target node found for key");
                    }
                }
            }
        }

        else{ // Redistribute 
            for (Map.Entry<String, String> entry : dataToRedistribute.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
        
                String keyHash = ConsistentHashing.getKeyHash(key);
                IECSNode targetNode = findNodeForKey(keyHash);
    
                SimpleKVCommunication.sendToServer(StatusType.PUT, key, null, getNodeByName(oldServer), LOGGER); // remove from old
    
                if (targetNode != null) {
                    SimpleKVCommunication.sendToServer(StatusType.PUT, key, value, targetNode, LOGGER);
                } else {
                    //LOGGER.log(Level.SEVERE, "No target node found for key: " + key);
                    LOGGER.error("No target node found for key");
                }
            }
        }
        
    }
    

    
    private IECSNode findNodeForKey(String keyHash) {
        // Logic to find the correct node for a given key hash based on the hash ring
        for (IECSNode node : nodes.values()) {
            if (ConsistentHashing.isKeyInRange(keyHash, node.getNodeHashRange())) {
                return node;
            }
        }
        return null; // This case should not occur if the hash ring is correctly maintained
    }
    
    private void handleClient(Socket clientSocket) {
        BufferedReader reader = null;
        String dead_server; 
        try {
            reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            String message = reader.readLine();
            System.out.println("Received from KVServer: " + message);
            dead_server = message.split(" ")[1]; 
           
            Collection<String> nodeNamesToRemove = new ArrayList<>();
            nodeNamesToRemove.add(dead_server); // Add the dead server to the collection
            boolean removeSuccess = removeNodes(nodeNamesToRemove); // Call the removeNodes method
            
            if (removeSuccess) {
                System.out.println("Node removed successfully: " + dead_server);
                // System.out.println("Now handling Dead Server's Storage"); 
                // handleStorageHandoff(dead_server);
            } else {
                System.out.println("Failed to remove node: " + dead_server);
            }

            if (nodes.isEmpty()) {
                System.out.println("No nodes are alive. Proceeding to stop services and shutdown ECS.");
            
                // Shutdown ECS
                boolean stopSuccess = stop();           
                boolean shutdownSuccess = shutdown();
                System.out.println("ECS shut down: " + shutdownSuccess);
                System.exit(0); 
            } else {
                System.out.println("There are still alive nodes. ECS will not shutdown.");
            }
            
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try {
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    


    /* ADD NODES */
    public boolean testConnection(IECSNode node) {
        // Attempt to open a socket to the node's host and port
        try (Socket socket = new Socket()) {
            // Connect with a timeout (e.g., 2000 milliseconds)
            socket.connect(new InetSocketAddress(node.getNodeHost(), node.getNodePort()), 2000);
            // Connection successful, node is reachable
            return true;
        } catch (IOException e) {
            // Connection failed, node is not reachable
            System.err.println("Failed to connect to node " + node.getNodeName() + " at " + node.getNodeHost() + ":" + node.getNodePort());
            return false;
        }
    }


    public static String getMD5Hash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(input.getBytes());
            BigInteger no = new BigInteger(1, messageDigest);
            String hashtext = no.toString(16);
            while (hashtext.length() < 32) {
                hashtext = "0" + hashtext;
            }
            return hashtext;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean start() {

        boolean allStarted = true;
        System.out.println("Attempting to start all nodes...");
        for (IECSNode node : nodes.values()) {
            System.out.println("Checking connection for node: " + node.getNodeName());
            boolean connected = false;
            for (int attempt = 0; attempt < 3; attempt++) {
                if (testConnection(node)) {
                    connected = true;
                    System.out.println("Successfully connected to node: " + node.getNodeName());
                    break; // Exit loop if connection is successful
                }
                try {
                    System.out.println("Connection attempt " + (attempt + 1) + " failed. Retrying...");
                    Thread.sleep(2000); // Wait before retrying
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("Thread was interrupted during sleep.");
                }
            }
            
            if (!connected) {
                System.err.println("Failed to connect to node " + node.getNodeName() + " after retries.");
                allStarted = false;
                continue;
            }

        }
        return allStarted;
    }



    @Override
    public boolean stop() {
        System.out.println("Stopping all nodes...");
        return true;
    }

    @Override
    public boolean shutdown() {
        // TODO
        System.out.println("Shutting down all nodes...");
        nodes.clear(); // Assuming nodes are removed from tracking as well
        return true;
    }

    private void computeAndSetNodeHash(ECSNode node) {
        String nodeHashString = getMD5Hash(node.getNodeName());
        BigInteger nodeHash = new BigInteger(nodeHashString, 16);
        hashRing.put(nodeHash, node);
    
        Map.Entry<BigInteger, IECSNode> lowerEntry = hashRing.lowerEntry(nodeHash);
        Map.Entry<BigInteger, IECSNode> higherEntry = hashRing.higherEntry(nodeHash);
    
        if (lowerEntry == null) {
            lowerEntry = hashRing.lastEntry(); // Wrap around the ring.
        }
        if (higherEntry == null) {
            higherEntry = hashRing.firstEntry(); // Wrap around the ring.
        }
    
        node.setHashRange(lowerEntry.getValue().getNodeHashRange()[1], higherEntry.getKey().toString(16));
    
        System.out.println("Node " + node.getNodeName() + " added with hash range: " + java.util.Arrays.toString(node.getNodeHashRange()));
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        String nodeHost = "localhost"; 
        int nodePort = 50000 + nodes.size(); // Ensure unique port numbers
        String nodeName = "Node_" + (nodes.size() + 1); 


        if (!nodes.containsKey(nodeName)){
            ECSNode node = new ECSNode(nodeName, nodeHost, nodePort, cacheStrategy, cacheSize, lowHashRange, highHashRange);
            setWriteLock(node, true);
        
            ecsHashRing.addNode(node); // Delegates to HashRing to handle hash and rebalance
            nodes.put(nodeName, node); // Keep track of nodes

            updateAllNodesConfiguration(); // Update and send configuration to all nodes to ensure consistency

            // redistribute Data After Node Addition
            for (IECSNode existed_node : nodes.values()) {
                // Exclude the new node to avoid fetching data that has just been initialized and is empty
                if (!existed_node.equals(node)) {
                    fetchServerData(existed_node);
                }
            }
            
            updateAllServersWithSuccessors(); // Update successors after adding the node
            System.out.println("Added Node: " + nodeName);
            return node;
        }

        else {
            System.out.println("Already part of the Server List"); 
            return nodes.get(nodeName);
        }
       
    }

    /* Overload addNode method */
    public IECSNode addNode(String cacheStrategy, int cacheSize, String nodeName) {
        // String nodeHost = "localhost"; 
        String[] parts = nodeName.split(":",2);
        String nodeHost = parts[0];
        int nodePort = Integer.parseInt(parts[1]);


        if (!nodes.containsKey(nodeName)){
            ECSNode node = new ECSNode(nodeName, nodeHost, nodePort, cacheStrategy, cacheSize, lowHashRange, highHashRange);
            setWriteLock(node, true);
        
            ecsHashRing.addNode(node); // Delegates to HashRing to handle hash and rebalance
            nodes.put(nodeName, node); // Keep track of nodes

            updateAllNodesConfiguration(); // Update and send configuration to all nodes to ensure consistency

            // redistribute Data After Node Addition
            for (IECSNode existed_node : nodes.values()) {
                // Exclude the new node to avoid fetching data that has just been initialized and is empty
                if (!existed_node.equals(node)) {
                    fetchServerData(existed_node);
                }
            }
            
            updateAllServersWithSuccessors(); // Update successors after adding the node
            System.out.println("Added Node: " + nodeName);
            return node;
        }

        else {
            System.out.println("Already part of the Server List"); 
            return nodes.get(nodeName);
        }
       
    }


    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        Collection<IECSNode> newNodes = new HashSet<IECSNode>(); 
        for (int i = 0; i < count; i++){
            IECSNode node = addNode(cacheStrategy, cacheSize); 
            sendConfiguration(node);
            newNodes.add(node); 
        }
        return newNodes; 
    }

    /* Overload addNodes method */
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize, String[] nodeNames) {
        Collection<IECSNode> newNodes = new HashSet<IECSNode>(); 
        for (int i = 0; i < count; i++){
            IECSNode node = addNode(cacheStrategy, cacheSize, nodeNames[i]); 
            sendConfiguration(node);
            newNodes.add(node); 
        }
        return newNodes; 
    }



    public void updateAllNodesConfiguration() {
        System.out.println("ECSClient, Updating all nodes"); 
        List<String> nodeNames = new ArrayList<>(nodes.keySet());
        System.out.println("Current Nodes in the System: " + nodeNames);

        StringBuilder allNodesMetadata = new StringBuilder(); // Build metadata string
        for (ECSNode node : ecsHashRing.getHashRing().values()) { // Fetch nodes directly from HashRing
            String[] hashRange = ecsHashRing.getHashRangeForNode(node.getNodeName());
            if (hashRange != null) {
                sendConfiguration(node, hashRange[0], hashRange[1]); // Apply updated hash range
                
                // Update node metadata
                String nodeMetadata = hashRange[0] + "," + hashRange[1] + "," + node.getNodeName() + ";";
                allNodesMetadata.append(nodeMetadata); 
            }
        }
        // Update metadata for all nodes and send to all nodes
        nodesMetadata = allNodesMetadata.toString();
        for (ECSNode node : ecsHashRing.getHashRing().values()) { // Fetch nodes directly from HashRing
            String[] hashRange = ecsHashRing.getHashRangeForNode(node.getNodeName());
            if (hashRange != null) {
                sendMetadata(node); // Send updated metadata
            }
        }
    }
    
    private void sendConfiguration(ECSNode node, String lowerHash, String upperHash) {
        String command = ECS_SECRET_TOKEN + " SET_CONFIG " + lowerHash + " " + upperHash;
        System.out.println("Sending command to KVServer: " + command);
        
        try (Socket socket = new Socket(node.getNodeHost(), node.getNodePort());
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            out.println(command);
            System.out.println("Configuration sent successfully to: " + node.getNodeName());
        } catch (IOException e) {
            System.err.println("Error sending configuration to node: " + e.getMessage());
        }
    }

    private void sendMetadata(ECSNode node) {
        String command = ECS_SECRET_TOKEN + " SET_METADATA " + nodesMetadata;
        System.out.println("Sending command to KVServer: " + command);
        
        try (Socket socket = new Socket(node.getNodeHost(), node.getNodePort());
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            out.println(command);
            System.out.println("Updated Metadata sent successfully to: " + node.getNodeName());
        } catch (IOException e) {
            System.err.println("Error sending updated metadata to node: " + e.getMessage());
        }
    }

    private void sendReadMetadata(ECSNode node) {
        String command = ECS_SECRET_TOKEN + " SET_READ_METADATA " + nodesReadMetadata;
        System.out.println("Sending command to KVServer: " + command);
        
        try (Socket socket = new Socket(node.getNodeHost(), node.getNodePort());
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            out.println(command);
            System.out.println("Updated Metadata sent successfully to: " + node.getNodeName());
        } catch (IOException e) {
            System.err.println("Error sending updated metadata to node: " + e.getMessage());
        }
    }

    private void setWriteLockAllNodes(boolean writeLock) {
        for (ECSNode node : ecsHashRing.getHashRing().values()) { // Fetch nodes directly from HashRing
            setWriteLock(node, writeLock);
        }
    }
    
    private void setWriteLock(ECSNode node, boolean writeLock) {
        String command = ECS_SECRET_TOKEN + " SET_WRITE_LOCK " + String.valueOf(writeLock);
        System.out.println("Sending command to KVServer: " + command);
        
        try (Socket socket = new Socket(node.getNodeHost(), node.getNodePort());
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            out.println(command);
            System.out.println("Write lock command sent successfully to: " + node.getNodeName());
        } catch (IOException e) {
            System.err.println("Error sending write lock command to node: " + e.getMessage());
        }
    }
    
    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO ; could be identical to addNodes in this simplified context               
        return addNodes(count, cacheStrategy, cacheSize);
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    public boolean removeNodes(Collection<String> nodeNames) {
        System.out.println("Removed nodes: " + nodeNames);
        for (String nodeName : nodeNames) {
            IECSNode iNode = nodes.get(nodeName); 
            if (iNode != null && iNode instanceof ECSNode) { 
                ECSNode node = (ECSNode) iNode; // Convert to ECSNode
                ecsHashRing.removeNode(node); // Remove ECSNode from HashRing
                nodes.remove(nodeName); // Remove node from tracking
            }
        }
        updateAllNodesConfiguration();
        updateAllServersWithSuccessors(); // Update successors after removing nodes
        return true;
    }

    public void sendConfiguration(IECSNode node) {
        String host = node.getNodeHost();
        int port = node.getNodePort();
        String[] hashRange = node.getNodeHashRange();
        String lowerHash = hashRange[0]; 
        String upperHash = hashRange[1]; 
    
        if (lowerHash == null || upperHash == null) {
            System.err.println("Hash range for node " + node.getNodeName() + " is incomplete.");
            return;
        }
    
        String command = ECS_SECRET_TOKEN + " SET_CONFIG " + lowerHash + " " + upperHash;
        System.out.println("Sending command to KVServer: " + command);
        
        try (Socket socket = new Socket(host, port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            out.println(command);
            System.out.println("Configuration sent successfully to: " + node.getNodeName());
        } catch (IOException e) {
            System.err.println("Error sending configuration to node: " + e.getMessage());
        }
    }

    
    private String serializeSuccessors(List<ECSNode> successors) {
        StringBuilder serialized = new StringBuilder();
        for (ECSNode node : successors) {
            if (serialized.length() > 0) {
                serialized.append(",");
            }
            serialized.append(node.getNodeHost()).append(":")
                      .append(node.getNodePort()).append(":")
                      .append(node.getNodeHashRange()[0]).append(":")
                      .append(node.getNodeHashRange()[1]);
        }
        return serialized.toString();
    }
    
    // Method to send a message to a KVServer
    private void sendMessageToKVServer(ECSNode kvServer, String message) {
        Socket socket = null;
        PrintWriter out = null;

        try {
            socket = new Socket(kvServer.getNodeHost(), kvServer.getNodePort());
            out = new PrintWriter(socket.getOutputStream(), true);
            out.println(ECS_SECRET_TOKEN + " " + message);
            LOGGER.info("Sent to KVServer: " + message);
        } catch (IOException e) {
            LOGGER.error("Error sending message to KVServer " + kvServer.getNodeName(), e);
        } finally {
            if (out != null) {
                out.close();
            }
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    LOGGER.error("Error closing socket", e);
                }
            }
        }
    }



    // Call this method after updating the hash ring to notify KVServer instances about their successors
    private void updateAllServersWithSuccessors() {
        // System.out.println("YOOOOOOOOO updateAllServersWithSuccessors YOOOOOOOOO");

        StringBuilder allNodesReadMetadata = new StringBuilder(); // Build metadata string
        for (Map.Entry<String, ECSNode> entry : ecsHashRing.getHashRing().entrySet()) {
            ECSNode serverNode = entry.getValue();
    
            // Compute successors for the serverNode
            List<ECSNode> successors = ecsHashRing.computeSuccessorsForNode(serverNode);
            String serializedSuccessors = serializeSuccessors(successors);
            
            // Send the serialized list of successors to the server
            sendMessageToKVServer(serverNode, "UPDATE_SUCCESSORS " + serializedSuccessors);

            // Add node to metadata
            String[] hashRange = ecsHashRing.getHashRangeForNode(serverNode.getNodeName());
            String nodeMetadata = hashRange[0] + "," + hashRange[1] + "," + serverNode.getNodeName() + ";";
            allNodesReadMetadata.append(nodeMetadata); 

            // Add successors to metadata
            for (ECSNode node : successors) {
                hashRange = ecsHashRing.getHashRangeForNode(node.getNodeName());
                nodeMetadata = hashRange[0] + "," + hashRange[1] + "," + node.getNodeName() + ";";
                allNodesReadMetadata.append(nodeMetadata); 
            }
        }

        // Update metadata for all nodes and send to all nodes
        nodesReadMetadata = allNodesReadMetadata.toString();
        for (ECSNode node : ecsHashRing.getHashRing().values()) { // Fetch nodes directly from HashRing
            String[] hashRange = ecsHashRing.getHashRangeForNode(node.getNodeName());
            if (hashRange != null) {
                sendReadMetadata(node); // Send updated metadata
            }
        }
    }
    

    ////////////////////////////////////////////////////////////////////////////////////

    public static void main(String[] args) {
        try {
            final int ecsPort = 51000;
            final ECSClient ecsClient = new ECSClient(ecsPort);
            
            // ecsClient.startListening(); 
            Thread listeningThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    ecsClient.startListening();
                }
            });
            listeningThread.start();
            ecsClient.monitorHeartbeats();

        
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
