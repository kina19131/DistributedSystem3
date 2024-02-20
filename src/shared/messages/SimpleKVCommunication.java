package shared.messages;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import java.io.IOException;

import shared.messages.SimpleKVMessage;
import shared.messages.KVMessage.StatusType;

public class SimpleKVCommunication {

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

    public static String receiveMessage(InputStream input, Logger logger) throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
		
		while(read != 13 && reading) {/* CR, LF, error */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 
			
			/* only read valid characters, i.e. letters and constants */
			if((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}
			
			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			
			/* read next char from stream */
			read = (byte) input.read();
		}
		
		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}
		
		msgBytes = tmp;
		
		/* build final String */
		String msg = new String(msgBytes);
		return msg;
    }

    public static SimpleKVMessage parseMessage(String msg, Logger logger) {
		logger.info("Received request string: " + msg);
        if (msg == null || msg.trim().isEmpty()) {
            logger.error("Empty or null request string received");
            return new SimpleKVMessage(StatusType.PUT_ERROR, null, null);
        }
        String[] parts = msg.split(" ", 3);
        StatusType status;
        try {
            status = StatusType.valueOf(parts[0]);
            logger.info("Parsed status: " + status);
        } catch (IllegalArgumentException e) {
            logger.error("Invalid Status:" + parts[0]);
            return new SimpleKVMessage(StatusType.PUT_ERROR, null, null);
        }
        
		SimpleKVMessage ret_msg;
		if (status == StatusType.SERVER_NOT_RESPONSIBLE || status == StatusType.SERVER_STOPPED || 
			status == StatusType.SERVER_WRITE_LOCK || status == StatusType.KEYRANGE_SUCCESS) {
				String parsed_msg = parts.length > 1 ? parts[1] : null;
				ret_msg = new SimpleKVMessage(status, parsed_msg);
				logger.info("Extracted message: " + msg);
		} else {
			String key = parts.length > 1 ? parts[1] : null;
        	String value = parts.length > 2 ? parts[2] : null;
			ret_msg = new SimpleKVMessage(status, key, value);
			logger.info("Extracted key: " + key + ", value: " + value);
		}
        
        return ret_msg;
    }

    public static void sendMessage(SimpleKVMessage msg, OutputStream output, Logger logger) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("Send message:\t '" + msg.getMsg() + "'");
    }
}