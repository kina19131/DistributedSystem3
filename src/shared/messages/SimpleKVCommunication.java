package shared.messages;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import java.io.IOException;

import shared.messages.SimpleKVMessage;
import shared.messages.KVMessage.StatusType;

import java.io.ByteArrayOutputStream;


public class SimpleKVCommunication {

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

	// public static String receiveMessage(InputStream input, Logger logger) throws IOException {
	// 	ByteArrayOutputStream messageBuffer = new ByteArrayOutputStream();
	// 	int read = input.read();
	// 	boolean reading = read != -1; // Continue reading if not end of stream
	// 	System.out.println("recieveMessag Boolean:"+ reading);
	// 	while (reading) {
	// 		System.out.println("in Reading");
	// 		// Check for CR or end of stream (-1)
	// 		if (read == 13 || read == -1) {
	// 			break; // Break the loop if CR or end of stream
	// 		}
	
	// 		// Valid character range check
	// 		if (read > 31 && read < 127) {
	// 			messageBuffer.write(read);
	// 		}
	
	// 		if (messageBuffer.size() >= DROP_SIZE) {
	// 			// Optional: Handle case where message exceeds DROP_SIZE
	// 			logger.warn("Message dropped due to exceeding DROP_SIZE limit.");
	// 			break;
	// 		}
	
	// 		read = input.read(); // Read next character
	// 	}
		
	// 	System.out.println("receiveMessage END");
	// 	return messageBuffer.toString();
	// }

	public static String receiveMessage(InputStream input, Logger logger) throws IOException {
		System.out.println("IN ReceiveMessage...1");
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();
		System.out.println("IN ReceiveMessage...2: " + read);
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
		System.out.println("IN ReceiveMessage...3: " + msg);
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
        String key = parts.length > 1 ? parts[1] : null;
        String value = parts.length > 2 ? parts[2] : null;
        logger.info("Extracted key: " + key + ", value: " + value);
        return new SimpleKVMessage(status, key, value);
    }

 

	public static void sendMessage(SimpleKVMessage msg, OutputStream output, Logger logger) throws IOException {
		String debugMsg = msg.getMsg().replace("\r", "\\r");
		System.out.println("SimpleKVComm, sendMsg, msg: " + debugMsg); 

		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes);
		output.flush(); // Make sure to flush the output stream to send the message immediately
		output.close();
		logger.info("Sent message: '" + msg.getMsg() + "'");
		System.out.println("SimpleKVComm, flush succeeded");
	}
	
}