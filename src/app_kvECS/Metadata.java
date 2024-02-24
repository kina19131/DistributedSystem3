package app_kvECS;

import java.util.Map;
import java.util.TreeMap;
import ecs.ECSNode;

public class Metadata {
    private TreeMap<String, ECSNode> hashRing = new TreeMap<>();

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
        String[] nodeHashRange = node.getHashRange();
        String nodeStartHash = nodeHashRange[0]; // Use the start hash as the key
        hashRing.put(nodeStartHash, node);
        rebalance();
    }

    // Example of a simplified rebalancing method
    private void rebalance() {
        String previousStartHash = null;
        ECSNode previousNode = null;
        for (Map.Entry<String, ECSNode> entry : hashRing.entrySet()) {
            ECSNode currentNode = entry.getValue();
            if (previousNode != null) {
                // Assuming you want to update the current node's range based on the previous node
                // You might need to adjust this logic to match your actual intent
                String[] currentRange = currentNode.getHashRange();
                currentRange[0] = previousStartHash; // Set the start of the current range to the previous node's start hash
                currentNode.setHashRange(currentRange[0], currentRange[1]); // Update the node's range
            }
            previousStartHash = entry.getKey(); // Update for the next iteration
            previousNode = currentNode;
        }
    
        // Handle the wrap-around logic here if needed
    }    
}
