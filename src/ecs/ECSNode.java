package ecs;

public class ECSNode implements IECSNode {
    private String nodeName;
    private String nodeHost;
    private int nodePort;
    private String cacheStrategy;
    private int cacheSize;
    private String hashRangeLower; // Lower bound of the hash range
    private String hashRangeUpper; // Upper bound of the hash range

    public ECSNode(String nodeName, String nodeHost, int nodePort, String cacheStrategy, int cacheSize, String lower, String upper) {
        this.nodeName = nodeName;
        this.nodeHost = nodeHost;
        this.nodePort = nodePort;
        this.cacheStrategy = cacheStrategy;
        this.cacheSize = cacheSize;
        this.hashRangeLower = lower;
        this.hashRangeUpper = upper;
    }

    public void setHashRange(String lowerBound, String upperBound) {
        this.hashRangeLower = lowerBound;
        this.hashRangeUpper = upperBound;
    }


    public String getCacheStrategy() {
        return this.cacheStrategy;
    }

    public int getCacheSize() {
        return this.cacheSize;
    }

    @Override
    public String getNodeName() {
        return this.nodeName;
    }

    @Override
    public String getNodeHost() {
        return this.nodeHost;
    }

    @Override
    public int getNodePort() {
        return this.nodePort;
    }

    @Override
    public String[] getNodeHashRange() {
        // Returns the hash range as an array for compatibility with the interface
        return new String[]{this.hashRangeLower, this.hashRangeUpper};
    }
}
