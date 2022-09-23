import com.sun.nio.sctp.MessageInfo;
import com.sun.nio.sctp.SctpChannel;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

// Enumeration to store message types
enum MessageType{string, control, launcher}

// Object to store message passing between nodes
// Message class can be modified to incoroporate all fields than need to be passed
// Message needs to be serializable
// Most base classes and arrays are serializable
public class Message implements Serializable 
{
	MessageType msgType;
	public Object message;
	List<Integer> vectorClock;
	// Constructor
	public Message(MessageType msgType, Object msg, List<Integer> vectorClock)
	{
		this.msgType = msgType;
		this.message = msg;
		this.vectorClock = vectorClock;
	}

	public Message(Object msg){
		this.msgType = MessageType.launcher;
		this.message = msg;
		this.vectorClock = null;
	}


	// Convert current instance of Message to ByteBuffer in order to send message over SCTP
	public ByteBuffer toByteBuffer() throws Exception
	{
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(this);
		oos.flush();
		
		ByteBuffer buf = ByteBuffer.allocateDirect(bos.size());
		buf.put(bos.toByteArray());
		
		oos.close();
		bos.close();

		// Buffer needs to be flipped after writing
		// Buffer flip should happen only once		
		buf.flip();
		return buf;
	}

	public void send(SctpChannel sc){
		try{
			ByteBuffer buf = this.toByteBuffer();
			MessageInfo messageInfo = MessageInfo.createOutgoing(null, 0);
			sc.send(buf, messageInfo);
		} catch (Exception e){
			e.printStackTrace();
		}
	}

	// Retrieve Message from ByteBuffer received from SCTP
	public static Message fromByteBuffer(ByteBuffer buf) throws Exception
	{
		// Buffer needs to be flipped before reading
		// Buffer flip should happen only once
		buf.flip();
		byte[] data = new byte[buf.limit()];
		buf.get(data);
		buf.clear();

		ByteArrayInputStream bis = new ByteArrayInputStream(data);
		ObjectInputStream ois = new ObjectInputStream(bis);
		Message msg = (Message) ois.readObject();

		bis.close();
		ois.close();

		return msg;
	}

	public static Message receiveMessage(SctpChannel channel) {
		try {
			ByteBuffer buf = ByteBuffer.allocateDirect(Node.MAX_MSG_SIZE);
			MessageInfo messageInfo = channel.receive(buf, null, null);
			return Message.fromByteBuffer(buf);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		return null;
	}
}
