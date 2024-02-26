package app_kvECS;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Collection;
import java.util.ArrayList;
import java.util.TreeMap; // Add import statement for TreeMap

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
    private Map<String, IECSNode> nodes = new HashMap<>();  //track the KVServer nodes
    private Metadata metadata = new Metadata();
    private String lowHashRange;
    private String highHashRange;

    private static final String ECS_SECRET_TOKEN = "secret";
    private TreeMap<BigInteger, IECSNode> hashRing = new TreeMap<>();

    private Map<String, String[]> nodeNameToHashRange = new HashMap<>();

    public String[] getHashRangeForNode(String nodeName) {
        return nodeNameToHashRange.get(nodeName);
    }
    
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

    

    private String getServerHash(String ip, int port) {
        return getMD5Hash(ip + ":" + port);
    }

    // private void computeAndSetNodeHash(ECSNode node) {
    //     String nodeHash = getMD5Hash(node.getNodeHost() + ":" + node.getNodePort());
    //     // Add the node to the metadata hash ring directly. The Metadata class will handle the placement and rebalancing.
    //     metadata.addNode(node);
    //     // Assuming Metadata.addNode(node) updates the node's hash range internally.
    //     System.out.println("Node added and hash range set by Metadata.");
    // }
    


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

            // // Proceed with sending the start command
            // try (Socket socket = new Socket(node.getNodeHost(), node.getNodePort())) {
            //     socket.setSoTimeout(2000); // Set a read timeout of 2000 milliseconds
            //     try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            //          BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            //         out.println("start");
            //         String response = in.readLine();
            //         if ("started".equals(response)) {
            //             System.out.println("Node " + node.getNodeName() + " started successfully.");
            //         } else {
            //             System.err.println("Node " + node.getNodeName() + " failed to start. Response: " + response);
            //             allStarted = false;
            //         }
            //     }
            // } catch (IOException e) {
            //     System.err.println("Error communicating with node " + node.getNodeName() + ": " + e.getMessage());
            //     allStarted = false;
            // }
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
            // This means the node should be placed at the start of the ring.
            lowerEntry = hashRing.lastEntry(); // Wrap around the ring.
        }
        if (higherEntry == null) {
            // This means the node should be placed at the end of the ring.
            higherEntry = hashRing.firstEntry(); // Wrap around the ring.
        }
    
        // Assuming setHashRange will update the node's range correctly.
        // You need to calculate and pass the correct lower and upper bounds based on your hash ring structure.
        node.setHashRange(lowerEntry.getValue().getNodeHashRange()[1], higherEntry.getKey().toString(16));
    
        System.out.println("Node " + node.getNodeName() + " added with hash range: " + java.util.Arrays.toString(node.getNodeHashRange()));
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        String nodeHost = "localhost"; 
        int nodePort = 50000 + nodes.size(); // Ensure unique port numbers
        String nodeName = "Node_" + (nodes.size() + 1); 
        ECSNode node = new ECSNode(nodeName, nodeHost, nodePort, cacheStrategy, cacheSize, lowHashRange, highHashRange); 
        
        metadata.addNode(node); // MODIFIED: Delegates to Metadata to handle hash and rebalance
        nodes.put(nodeName, node); // Keep track of nodes
        
        // MODIFIED: Update and send configuration to all nodes to ensure consistency
        updateAllNodesConfiguration(); 
        
        System.out.println("Added Node: " + nodeName);
        return node;
    }
    
    // MODIFIED: Revise the updateAllNodesConfiguration method to fetch and apply updated hash ranges
    public void updateAllNodesConfiguration() {
        for (ECSNode node : metadata.getHashRing().values()) { // Fetch nodes directly from Metadata
            String[] hashRange = metadata.getHashRangeForNode(node.getNodeName());
            if (hashRange != null) {
                sendConfiguration(node, hashRange[0], hashRange[1]); // Apply updated hash range
            }
        }
    }
    
    // MODIFIED: Adjust sendConfiguration to directly use provided hash range parameters
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
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        Collection<IECSNode> newNodes = new HashSet<IECSNode>(); 
        for (int i = 0; i < count; i++){
            IECSNode node = addNode(cacheStrategy, cacheSize); 
            // computeAndSetNodeHash((ECSNode) node);
            // Now send configuration
            sendConfiguration(node);
            newNodes.add(node); 
        }
        return newNodes; 
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
        for (String nodeName : nodeNames) {
            IECSNode iNode = nodes.get(nodeName); // Retrieve IECSNode
            if (iNode != null && iNode instanceof ECSNode) { // Check if it's an instance of ECSNode
                ECSNode node = (ECSNode) iNode; // Convert to ECSNode
                metadata.removeNode(node); // Remove ECSNode from metadata
                nodes.remove(nodeName); // Remove node from tracking
            }
        }

        updateAllNodesConfiguration();
        System.out.println("Removed nodes: " + nodeNames);
        return true;
    }


    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        // Simplified: return any node for demonstration
        return new HashMap<String, IECSNode>(nodes); 
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
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
    
    // public void updateAllNodesConfiguration() {
    //     for (Map.Entry<String, IECSNode> entry : nodes.entrySet()) {
    //         ECSNode node = (ECSNode) entry.getValue();
    //         System.out.print("Update NODE: " + node.getNodeName()); 
    //         sendConfiguration(node);
    //     }
    // }
    
    

    public static void main(String[] args) {
        try {
            ECSClient ecsClient = new ECSClient();
    
            // Test adding nodes
            System.out.println("Adding nodes...");
            Collection<IECSNode> addedNodes = ecsClient.addNodes(3, "FIFO", 1024);
            System.out.println("Added nodes: " + addedNodes.size());
    
            // Test starting the service
            System.out.println("Starting the service...");
            boolean startSuccess = ecsClient.start();
            System.out.println("Service started: " + startSuccess);
    
            // // Test stopping the service
            // System.out.println("Stopping the service...");
            // boolean stopSuccess = ecsClient.stop();
            // System.out.println("Service stopped: " + stopSuccess);
    
            // // Prepare a collection of node names to be removed
            // Collection<String> nodeNamesToRemove = new ArrayList<String>();
            // for (IECSNode node : addedNodes) {
            //     nodeNamesToRemove.add(node.getNodeName());
            // }
    
            // // Test removing nodes
            // System.out.println("Removing nodes...");
            // boolean removeSuccess = ecsClient.removeNodes(nodeNamesToRemove);
            // System.out.println("Nodes removed: " + removeSuccess);
    
            // // Test shutting down the service
            // System.out.println("Shutting down the service...");
            // boolean shutdownSuccess = ecsClient.shutdown();
            // System.out.println("Service shut down: " + shutdownSuccess);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
