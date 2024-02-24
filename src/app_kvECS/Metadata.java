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
        
        String nodeHash = node.getHashRange();
        hashRing.put(nodeHash, node);
        rebalance();
    }

    // Example of a simplified rebalancing method
    private void rebalance() {
        ECSNode previousNode = null;
        for (Map.Entry<String, ECSNode> entry : hashRing.entrySet()) {
            ECSNode currentNode = entry.getValue();
            if (previousNode != null) {
                // Update the range of the current node based on the previous node's range
                String newStartRange = previousNode.getHashRange();
                currentNode.updateKeyRange(newStartRange);
            }
            // TODO: need to communicate with the server to actually move the keys
            // (involve network communication)
            previousNode = currentNode;
        }
        
        // Handle wrap-around of the last node to the first
        if (previousNode != null && !hashRing.isEmpty()) {
            ECSNode firstNode = hashRing.firstEntry().getValue();
            previousNode.updateKeyRange(firstNode.getHashRange());
            // Again, initiate the actual key transfer between the servers
        }
    }
}
