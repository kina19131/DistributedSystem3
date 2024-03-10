package ecs;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ConsistentHashing {

    public static String getKeyHash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(key.getBytes());
            BigInteger no = new BigInteger(1, messageDigest);
            // while (no.toString(16).length() < 32) {
            //     no = new BigInteger("0" + no.toString(16), 16);
            // }
            // return no.toString(16);
			return String.format("%032x", no);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isKeyInRange(String keyHash, String[] keyRange) {
		System.out.println("Entering isKeyInRange with keyHash: " + keyHash + " and keyrange:" + keyRange[0] +","+keyRange[1]);
		if (keyRange[0] == null && keyRange[1] == null) {
			return false;
		}
		BigInteger hash = new BigInteger(keyHash, 16);
		BigInteger lowEnd = new BigInteger(keyRange[0], 16);
		BigInteger highEnd = new BigInteger(keyRange[1], 16);
	
		System.out.println("KVServer, isKeyInRange, SERVER low: " + lowEnd);
		System.out.println("KVServer, isKeyInRange, SERVER high: " + highEnd);
	
		// Check if the range wraps around the hash space
		if (lowEnd.compareTo(highEnd) < 0) {
			System.out.println("(case 1) Normal range: does not wrap around the hash ring");
			// "In range" if the hash is greater than or equal to lowEnd and less than highEnd
			return hash.compareTo(lowEnd) >= 0 && hash.compareTo(highEnd) < 0;
		} else {
			System.out.println("(case 2) Wrap-around range: wraps around the hash ring");
			// "In range" if the hash is greater than or equal to lowEnd OR less than highEnd
			// This handles the wrap-around scenario correctly
			return hash.compareTo(lowEnd) >= 0 || hash.compareTo(highEnd) < 0;
		}
	}
    
}
