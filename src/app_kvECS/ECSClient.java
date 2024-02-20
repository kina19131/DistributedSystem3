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

public class ECSClient implements IECSClient {
    private Map<String, IECSNode> nodes = new HashMap<>();  //track the KVServer nodes
    private Metadata metadata = new Metadata();
    private String lowHashRange;
    private String highHashRange;

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

    // This will be called when adding a new node to compute its position
    private void computeAndSetNodeHash(ECSNode node) {
       String nodeHash = getMD5Hash(node.getNodeHost() + ":" + node.getNodePort());
        
        // Find the correct position in the ring for this node
        String lowerBound = metadata.getLowerBound(nodeHash);
        String upperBound = metadata.getUpperBound(nodeHash);
        
        node.setHashRange(lowerBound, upperBound);
        metadata.addNode(node);
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

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        String nodeHost = "localhost"; 
        int nodePort = 50000 + nodes.size(); 
        String nodeName = "Node_" + (nodes.size() + 1); 
        ECSNode node = new ECSNode(nodeName, nodeHost, nodePort, cacheStrategy, cacheSize, lowHashRange, highHashRange); 


        computeAndSetNodeHash(node);

        nodes.put(nodeName, node); 
        metadata.addNode(node); 
        
        System.out.println("Added Node: " + nodeName); 
        return node; 
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        Collection<IECSNode> newNodes = new HashSet<IECSNode>(); 
        for (int i = 0; i < count; i++){
            IECSNode node = addNode(cacheStrategy, cacheSize); 
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

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        for (String nodeName : nodeNames) {
            nodes.remove(nodeName);
        }
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


    public static void main(String[] args) {
        try {
            ECSClient ecsClient = new ECSClient();
    
            // Test adding nodes
            System.out.println("Adding nodes...");
            Collection<IECSNode> addedNodes = ecsClient.addNodes(2, "FIFO", 1024);
            System.out.println("Added nodes: " + addedNodes.size());
    
            // Test starting the service
            System.out.println("Starting the service...");
            boolean startSuccess = ecsClient.start();
            System.out.println("Service started: " + startSuccess);
    
            // Test stopping the service
            System.out.println("Stopping the service...");
            boolean stopSuccess = ecsClient.stop();
            System.out.println("Service stopped: " + stopSuccess);
    
            // Prepare a collection of node names to be removed
            Collection<String> nodeNamesToRemove = new ArrayList<String>();
            for (IECSNode node : addedNodes) {
                nodeNamesToRemove.add(node.getNodeName());
            }
    
            // Test removing nodes
            System.out.println("Removing nodes...");
            boolean removeSuccess = ecsClient.removeNodes(nodeNamesToRemove);
            System.out.println("Nodes removed: " + removeSuccess);
    
            // Test shutting down the service
            System.out.println("Shutting down the service...");
            boolean shutdownSuccess = ecsClient.shutdown();
            System.out.println("Service shut down: " + shutdownSuccess);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
