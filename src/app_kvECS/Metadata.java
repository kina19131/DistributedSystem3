package app_kvECS;

import java.util.Map;
import java.util.TreeMap;
import ecs.ECSNode;


import java.net.Socket;
import java.io.*;

public class Metadata {
    private TreeMap<String, ECSNode> hashRing = new TreeMap<>();

    public TreeMap<String, ECSNode> getHashRing() { // Accessor for ECSClient to iterate over hashRing if necessary
        return hashRing;
    }

    public boolean isHashRingEmpty() {
        return hashRing.isEmpty();
    }

    public String getLowerBound(String nodeHash) {
        Map.Entry<String, ECSNode> lowerEntry = hashRing.lowerEntry(nodeHash);
        if (lowerEntry == null) {
            lowerEntry = hashRing.lastEntry();
        }
        return lowerEntry != null ? lowerEntry.getKey() : null;
    }

    public String getUpperBound(String nodeHash) {
        Map.Entry<String, ECSNode> higherEntry = hashRing.higherEntry(nodeHash);
        if (higherEntry == null) {
            higherEntry = hashRing.firstEntry();
        }
        return higherEntry != null ? higherEntry.getKey() : null;
    }

    public void addNode(ECSNode node) {
        // Compute the node's hash and add it to the hash ring
        String nodeHash = ECSClient.getMD5Hash(node.getNodeHost() + ":" + node.getNodePort());
        hashRing.put(nodeHash, node);
        rebalance(); // Adjust hash ranges for all nodes
    }
    
    public void removeNode(ECSNode node) {
        // Compute the node's hash and remove it from the hash ring
        String nodeHash = ECSClient.getMD5Hash(node.getNodeHost() + ":" + node.getNodePort());
        hashRing.remove(nodeHash);
        rebalance(); // Adjust hash ranges after removal
    }

    private void rebalance() {
        sendStatusToECS(); 
        if (hashRing.isEmpty()) {

            // BIZARRE IT ENTERS HERE 
            System.out.println("Hash ring is empty. No rebalance needed.");
            return; // Guard against empty hash ring
        }
    
        String firstHash = hashRing.firstKey();
        String lastHash = hashRing.lastKey();
        Map.Entry<String, ECSNode> previousEntry = hashRing.lowerEntry(firstHash); // This should wrap around to the last entry
    
        if (previousEntry == null) {
            previousEntry = hashRing.lastEntry(); // Ensure wrap-around if lowerEntry doesn't work as expected
        }

        System.out.println("Rebalancing. First hash: " + firstHash + ", Last hash: " + lastHash);

    
        // Print initial state
        System.out.println("Starting rebalance. First hash: " + firstHash + ", Last hash: " + lastHash);
        System.out.println("Initial previous entry: " + previousEntry.getKey());
    
        for (Map.Entry<String, ECSNode> currentEntry : hashRing.entrySet()) {
            String currentHash = currentEntry.getKey();
            ECSNode currentNode = currentEntry.getValue();
            
            String lowerBound = previousEntry.getKey();
            String upperBound = currentHash;
    
            currentNode.setHashRange(lowerBound, upperBound); // Update the node's hash range
    
            // Diagnostic print
            System.out.println("Node: " + currentNode.getNodeName() + ", Lower Bound: " + lowerBound + ", Upper Bound: " + upperBound);
    
            previousEntry = currentEntry; // Move to the next node
        }
    
        // Special handling for the first node to ensure wrap-around logic
        ECSNode firstNode = hashRing.get(firstHash);
        firstNode.setHashRange(lastHash, firstHash); // Wrap around: from last to first
    
        // Diagnostic print for the first node wrap-around
        System.out.println("First Node Wrap-around: " + firstNode.getNodeName() + ", Lower Bound: " + lastHash + ", Upper Bound: " + firstHash);
    }
    
    public String[] getHashRangeForNode(String nodeName) {
        // Iterate through hashRing to find the node by name and return its hash range
        for (ECSNode node : hashRing.values()) {
            if (node.getNodeName().equals(nodeName)) {
                return new String[]{node.getNodeHashRange()[0], node.getNodeHashRange()[1]};
            }
        }
        return null; // Node not found
    }

    private void sendStatusToECS() {
        String command = "SERVER_WRITE_LOCK";
        System.out.println("Metdata -> ECSClient: " + command);
        
        try (Socket socket = new Socket("localhost", 51000); // Define ECSClient 
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            out.println(command);
            System.out.println("Rebalance, Status update from Metadata to ECSClient");
        } catch (IOException e) {
            System.err.println("Error sending configuration to node: " + e.getMessage());
        }
    }
}
