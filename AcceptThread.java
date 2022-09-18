/*
 * Ethan Cooper
 * ewc180001
 * CS 6378.001
 */
import com.sun.nio.sctp.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class AcceptThread extends Thread {
    private Node node;
    private int portNum;

    public AcceptThread(Node ownNode, int portNum) {
        this.ownNode = ownNode;
        this.portNum = portNum;
    }

    public void run() {
        try {
            InetSocketAddress addr = new InetSocketAddress(portNum); // Get address from port number
            SctpServerChannel ssc = SctpServerChannel.open(); //Open server channel
            ssc.bind(addr);//Bind server channel to address


            SctpChannel sc = ssc.accept();
            // DO I NEED TO SLEEP HERE?
            // Should get a message immediately from client with the nodeNum of the remote device
            ByteBuffer buf = ByteBuffer.allocateDirect(Node.MAX_MSG_SIZE);
            sc.receive(buf, null, null); // Messages are received over SCTP using ByteBuffer
            String msg = Message.fromByteBuffer(buf).message;
            System.out.println("Message received from node: " + msg);

            // parse int from the message
            int connectedNodeNum = Integer.parseInt(msg);
            node.addChannel(connectedNodeNum, sc);
            synchronized(node) {
                node.notify();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}