package shared.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ecs.ConsistentHashing;

public class Metadata {
    
    private Map<String, List<String[]>> metadataStore = new HashMap<>();
    private String metadataStr = null;


    /* Set the metadata */
	public String setMetadata(String metadata) {
        // Set string
        metadataStr = metadata;

        // Set the metadata data structure
		String[] nodes = metadata.split(";");
		
		for (String node : nodes) {
			String[] nodeDetails = node.split(",");
			String nodeHost = nodeDetails[2];

			String[] hashrange = {nodeDetails[0], nodeDetails[1]};

            List<String[]> ranges = metadataStore.get(nodeHost);
            if (ranges == null) {
                ranges = new ArrayList<>();
                metadataStore.put(nodeHost, ranges);
            }
            ranges.add(hashrange);
		}
		return null;
	}

    /* Get metadata in string format */
    public String getMetadataString() {
        return metadataStr;
    }

    /* Returns the list of hash ranges the server is responsible for */
    public List<String[]> getServerHashRanges(String server) {
        return metadataStore.get(server);
    }

    /* Returns if server is responsible to handle the key */
    public boolean isServerResponsible(String server, String key) {        
        String keyHash = ConsistentHashing.getKeyHash(key);

        List<String[]> ranges = getServerHashRanges(server);
        for (String[] hashrange : ranges) {
            if (ConsistentHashing.isKeyInRange(keyHash, hashrange)) {
                return true;
            }
        }
        return false;
    }

    /* Parse metadata string to find the responsible server */
    public String findResponsibleServer(String key) {
        String keyHash = ConsistentHashing.getKeyHash(key);
        
        for (Map.Entry<String, List<String[]>> entry : metadataStore.entrySet()) {
            for (String[] hashrange : entry.getValue()) {
                if (ConsistentHashing.isKeyInRange(keyHash, hashrange)) {
                    return entry.getKey();
                }
            }
            
        }
        return null;
    }
}
