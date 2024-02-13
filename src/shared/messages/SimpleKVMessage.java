package shared.messages;

public class SimpleKVMessage implements KVMessage {

    private String key;
    private String value;
    private StatusType status;
    private String msg;
    private byte[] msgBytes;
	private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;

    public SimpleKVMessage(StatusType status, String key, String value) {
        this.status = status;
        this.key = key;
        this.value = value;

        this.msg = status.name() + " " + key + " " + (value != null ? value : "");
        this.msg.trim();
        this.msgBytes = toByteArray(this.msg);
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public StatusType getStatus() {
        return status;
    }

    public String getMsg() {
		return msg;
	}

    /**
	 * Returns an array of bytes that represent the ASCII coded message content.
	 * 
	 * @return the content of this message as an array of bytes 
	 * 		in ASCII coding.
	 */
    public byte[] getMsgBytes() {
		return msgBytes;
	}

    private byte[] toByteArray(String s){
		byte[] bytes = s.getBytes();
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;		
	}

    private byte[] addCtrChars(byte[] bytes) {
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;		
	}

}