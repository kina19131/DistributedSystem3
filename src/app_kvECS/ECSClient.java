package app_kvECS;

import java.net.ServerSocket;

import java.util.List;
import java.util.ArrayList;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Collection;
import java.util.ArrayList;
import java.util.TreeMap; // Add import statement for TreeMap

import java.util.logging.Logger;
import java.util.logging.Level;


import java.net.Socket;
import java.io.*;

import ecs.ECSNode;
import java.net.InetSocketAddress;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;

import ecs.IECSNode;

import shared.messages.KVMessage;
import shared.messages.SimpleKVMessage;

public class ECSClient implements IECSClient {
    private int ecsPort;

    private Map<String, IECSNode> nodes = new HashMap<>();  //track the KVServer nodes
    private Metadata metadata = new Metadata();
    private String lowHashRange;
    private String highHashRange;

    private static final String ECS_SECRET_TOKEN = "secret";
    private TreeMap<BigInteger, IECSNode> hashRing = new TreeMap<>();

    private Map<String, String[]> nodeNameToHashRange = new HashMap<>();

    private static final Logger LOGGER = Logger.getLogger(ECSClient.class.getName());



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

    private String getServerHash(String ip, int port) {
        return getMD5Hash(ip + ":" + port);
    }



    

    public ECSClient(int ecsPort){
        this.ecsPort = ecsPort; 
    }

    public void startListening() {
        try (ServerSocket serverSocket = new ServerSocket(ecsPort)) {
            LOGGER.info("ECSClient listening on port " + ecsPort);

            while (true) { // Continuously accept new connections
                try (Socket clientSocket = serverSocket.accept();
                     BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
                    String inputLine = in.readLine();
                    if (inputLine != null && inputLine.startsWith("ALIVE")) {
                        System.out.println("SERVER SENT ALIVE MSG, Adding node...");
                        Collection<IECSNode> addedNodes = addNodes(1, "FIFO", 1024);
                        System.out.println("Added nodes: " + addedNodes.size());
                    }

                    if (inputLine != null && inputLine.startsWith("STORAGE_HANDOFF")) {

                        String dead_server = inputLine.split(" ")[1]; 
           
                        Collection<String> nodeNamesToRemove = new ArrayList<>();
                        nodeNamesToRemove.add(dead_server); // Add the dead server to the collection
                        boolean removeSuccess = removeNodes(nodeNamesToRemove); // Call the removeNodes method
                        
                        if (removeSuccess) {
                            System.out.println("Node removed successfully: " + dead_server);
                            processStorageHandoff(dead_server, inputLine.split(" ")[2]);
                            
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
                        

                        
                        
                    }
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "Error processing connection", e);
                }
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Could not listen on port " + ecsPort, e);
        }
    }


    /* Handle Handed off Storage */
    private void processStorageHandoff(String serverName, String serializedData) {
        Map<String, String> dataToRedistribute = deserializeStorage(serializedData);
        // Now you have the deserialized storage map from serverName, process it as needed
        LOGGER.info("Received storage handoff from " + serverName + ": " + dataToRedistribute);
        redistributeData(dataToRedistribute);
    }

    private Map<String, String> deserializeStorage(String serializedData) {
        Map<String, String> storage = new HashMap<>();
        String[] entries = serializedData.split(";");
        for (String entry : entries) {
            String[] keyValue = entry.split("=");
            if (keyValue.length == 2) {
                storage.put(keyValue[0], keyValue[1]);
            }
        }
        return storage;
    }

    // private void redistributeData(Map<String, String> dataToRedistribute) {
    //     for (Map.Entry<String, String> entry : dataToRedistribute.entrySet()) {
    //         String key = entry.getKey();
    //         String value = entry.getValue();

    //         BigInteger keyHash = key_getMD5Hash(key);
    //         for (IECSNode node : nodes.values()) {
    //             BigInteger rangeStart = new BigInteger(node.getNodeHashRange()[0], 16);
    //             BigInteger rangeEnd = new BigInteger(node.getNodeHashRange()[1], 16);

    //             if (keyHash.compareTo(rangeStart) >= 0 && keyHash.compareTo(rangeEnd) <= 0) {
    //                 sendToServer(node, key, value);
    //             }
    //         }
    //     }
    // }

    private void redistributeData(Map<String, String> dataToRedistribute) {
        for (Map.Entry<String, String> entry : dataToRedistribute.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
    
            // Use the BigInteger returned by key_getMD5Hash directly
            BigInteger keyHash = key_getMD5Hash(key);
            IECSNode targetNode = null;
    
            // Iterate over the nodes to find the correct node for this key
            for (IECSNode node : nodes.values()) {
                BigInteger lowEnd = new BigInteger(node.getNodeHashRange()[0], 16);
                BigInteger highEnd = new BigInteger(node.getNodeHashRange()[1], 16);
    
                // Check if the key's hash is within the node's range, considering wrap-around
                boolean isInRange;
                if (lowEnd.compareTo(highEnd) < 0) {
                    isInRange = keyHash.compareTo(lowEnd) >= 0 && keyHash.compareTo(highEnd) < 0;
                } else {
                    isInRange = keyHash.compareTo(lowEnd) >= 0 || keyHash.compareTo(highEnd) < 0;
                }
    
                if (isInRange) {
                    targetNode = node;
                    break;
                }
            }
    
            // If a target node is found, send the key-value pair to that node
            if (targetNode != null) {
                sendToServer(targetNode, key, value);
            }
        }
    }
    
    
    

    private void sendToServer(IECSNode node, String key, String value) {
        try (Socket socket = new Socket(node.getNodeHost(), node.getNodePort());
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            out.println("PUT " + key + " " + value);
            System.out.println("REDIST SENT TO SERVER");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private BigInteger key_getMD5Hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(key.getBytes());
            return new BigInteger(1, messageDigest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 hashing error", e);
        }
    }
    /* Complete  */


    
    
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
        // TODO
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
        String nodeHashString = getMD5Hash(node.getNodeHost() + ":" + node.getNodePort());
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
        
            metadata.addNode(node); // Delegates to Metadata to handle hash and rebalance
            nodes.put(nodeName, node); // Keep track of nodes
            
            // Update and send configuration to all nodes to ensure consistency
            updateAllNodesConfiguration(); 
            
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

    

    public void updateAllNodesConfiguration() {
        System.out.println("ECSClient, Updating all nodes"); 
        List<String> nodeNames = new ArrayList<>(nodes.keySet());
        System.out.println("Current Nodes in the System: " + nodeNames);

        for (ECSNode node : metadata.getHashRing().values()) { // Fetch nodes directly from Metadata
            String[] hashRange = metadata.getHashRangeForNode(node.getNodeName());
            if (hashRange != null) {
                sendConfiguration(node, hashRange[0], hashRange[1]); // Apply updated hash range
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
                metadata.removeNode(node); // Remove ECSNode from metadata
                nodes.remove(nodeName); // Remove node from tracking
            }
        }
        updateAllNodesConfiguration();
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
    

    public static void main(String[] args) {
        try {
            int ecsPort = 51000;
            ECSClient ecsClient = new ECSClient(ecsPort);
            
            // Adding
            System.out.println("Adding nodes...");
            Collection<IECSNode> addedNodes = ecsClient.addNodes(2, "FIFO", 1024);
            System.out.println("Added nodes: " + addedNodes.size());
    
            ecsClient.startListening(); 

        
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}


/* Next to do: Let KVServer tell ECSClient when it becomes avaialbe, and add the node to NodeList then get rebalanced */