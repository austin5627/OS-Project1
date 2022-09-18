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
    private SctpChannel sc;
    final Node ownNode;
    private int connectedNode;


    public ListenerThread(Node ownNode, SctpChannel sc, int connectedNode) {
        this.ownNode = ownNode;
        this.sc = sc;
        this.connectedNode = connectedNode;
    }

    public void run() {

        try {

            ByteBuffer buf = ByteBuffer.allocateDirect(Node.MAX_MSG_SIZE); // Messages are received over SCTP using ByteBuffer
            int msgsReceived = 0;
            int sum = 0;
            sc.configureBlocking(true); // Ensures that the channel will block until a message is received
            while (msgsReceived < 50) {
                // listen for msg
                sc.receive(buf, null, null);
                String message = Message.fromByteBuffer(buf).message;
                if (!ownNode.active.getAndSet(true)){
                    notifyNode();
                }

                if (message.equals("TERMINATE")){
                    keepListening = false;
                }
            }
            System.out.println("Received all messages");
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Threads automatically terminate after finishing run
    }

    public void notifyNode() {
        synchronized(ownNode) {
            ownNode.notify();
        }
    }
}