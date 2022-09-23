/*
 * Ethan Cooper
 * ewc180001
 * CS 6378.001
 */
import com.sun.nio.sctp.*;

import java.nio.ByteBuffer;
import java.util.Arrays;

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
            sc.configureBlocking(true); // Ensures that the channel will block until a message is received
            boolean keepListening = true;
            while (keepListening) {
                // listen for msg
                Message message = Message.receiveMessage(sc);
                if (message.msgType == MessageType.control) {
                    if (message.message.equals("TERMINATE")){
                        keepListening = false;
                    }

                } else if (message.msgType == MessageType.application){
                    int[] msgVectClock = message.vectorClock;
                    System.out.println("Received: " + message + " with vector clock: " + Arrays.toString(msgVectClock));
                    ownNode.syncSet(msgVectClock);
                    if (!ownNode.active.getAndSet(true)){
                        notifyNode();
                    }
                }

            }
            System.out.println("Received all messages");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        // Threads automatically terminate after finishing run
    }

    public void notifyNode() {
        synchronized(ownNode) {
            ownNode.notify();
        }
    }
}
