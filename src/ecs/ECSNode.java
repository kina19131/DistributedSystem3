package ecs;

public class ECSNode implements IECSNode {
    private String nodeName;
    private String nodeHost;
    private int nodePort;
    // These fields are not part of the IECSNode interface but can be used for your internal logic.
    private String cacheStrategy;
    private int cacheSize;
    // Assuming the hash range is represented by two strings, e.g., ["lowHashValue", "highHashValue"]
    private String[] nodeHashRange = new String[2];

    public ECSNode(String nodeName, String nodeHost, int nodePort, String cacheStrategy, int cacheSize, String lowHashRange, String highHashRange) {
        this.nodeName = nodeName;
        this.nodeHost = nodeHost;
        this.nodePort = nodePort;
        this.cacheStrategy = cacheStrategy;
        this.cacheSize = cacheSize;
        this.nodeHashRange[0] = lowHashRange;
        this.nodeHashRange[1] = highHashRange;
    }


    public void setHashRange(String lowerBound, String upperBound) { // Set the hash range for this node
        this.nodeHashRange[0] = lowerBound;
        this.nodeHashRange[1] = upperBound;
    }

    // Get the hash range as a concatenated string
    public String getHashRange() {
        return nodeHashRange[0] + "," + nodeHashRange[1];
    }

    // Update the hash range based on a new range provided as a single string
    public void updateKeyRange(String newRange) {
        String[] parts = newRange.split(",");
        this.nodeHashRange[0] = parts.length > 0 ? parts[0] : "";
        this.nodeHashRange[1] = parts.length > 1 ? parts[1] : "";
    }

    // Additional getters for cacheStrategy and cacheSize if needed
    public String getCacheStrategy() {
        return this.cacheStrategy;
    }

    public int getCacheSize() {
        return this.cacheSize;
    }




    @Override
    public String getNodeName() {
        return nodeName;
    }

    @Override
    public String getNodeHost() {
        return nodeHost;
    }

    @Override
    public int getNodePort() {
        return nodePort;
    }

    @Override
    public String[] getNodeHashRange() {
        return nodeHashRange;
    }

    // If needed, add getters for cacheStrategy and cacheSize
}
