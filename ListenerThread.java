/*
 * Ethan Cooper
 * ewc180001
 * CS 6378.001
 */
import java.io.IOException;
import com.sun.nio.sctp.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class ListenerThread extends Thread {
    private Node node;
    private int connectedNode;
    private InetSocketAddress addr;
    private int portNum;

    public ListenerThread(Node node, int connectedNode) {
        this.node = node;
        this.connectedNode = connectedNode;
        this.portNum = node.getPort();
        addr = new InetSocketAddress("dc0" + connectedNode + ".utdallas.edu", portNum);
    }

    public void run() {
        MessageInfo messageInfo = MessageInfo.createOutgoing(null, 0); // MessageInfo for SCTP layer
        Message msg = new Message(String.valueOf(node.getNodeId()));
        SctpChannel sc;
        try {
            // A node will accept connections from other nodes with a lower number
            // A node will try to connect to nodes with a higher number
            if (node.getNodeId() < connectedNode) {
                sc = SctpChannel.open(addr, 0, 0);
                node.addChannel(connectedNode, sc); // Connect to server using the address
                sc.send(msg.toByteBuffer(), messageInfo); // Messages are sent over SCTP using ByteBuffer
                System.out.println("\t Message sent to node " + connectedNode + ": " + msg.message);
                // Waking up the main thread so it can check if all connections have been established
                notifyNode();
            }
            else {
                while (!node.containsChannel(connectedNode)) {
                    Thread.sleep(100);
                }
                sc = node.getChannel(connectedNode);
            }

            ByteBuffer buf = ByteBuffer.allocateDirect(Node.MAX_MSG_SIZE); // Messages are received over SCTP using ByteBuffer
            int msgsReceived = 0;
            int sum = 0;
            sc.configureBlocking(true); // Ensures that the channel will block until a message is received
            while (msgsReceived < 50) {
                // listen for msg
                sc.receive(buf, null, null);
                sum += Integer.parseInt(Message.fromByteBuffer(buf).message);
                msgsReceived++;
            }
            System.out.println("Received all messages");


            //node.putSum(connectedNode, sum);

            node.finishedListening();
            notifyNode();
        } catch (Exception e) {
            System.out.println("Exception in ListenerThread");
            System.out.println(e);
        }

        // Threads automatically terminate after finishing run
    }

    public void notifyNode() {
        synchronized(node) {
            node.notify();
        }
    }
}