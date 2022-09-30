/*
 * Ethan Cooper
 * ewc180001
 * CS 6378.001
 */
import com.sun.nio.sctp.*;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;

public class ListenerThread extends Thread {
    private SctpChannel sc;
    final Node ownNode;
    private int connectedNode;

    private boolean keepListening = true;
    public ListenerThread(Node ownNode, SctpChannel sc, int connectedNode) {
        this.ownNode = ownNode;
        this.sc = sc;
        this.connectedNode = connectedNode;
    }

    public void run() {

        try {

            ByteBuffer buf = ByteBuffer.allocateDirect(Node.MAX_MSG_SIZE); // Messages are received over SCTP using ByteBuffer
            sc.configureBlocking(true); // Ensures that the channel will block until a message is received
            while (keepListening && sc.isOpen()) {
                // listen for msg
                Message message = Message.receiveMessage(sc);
                if (message == null) {
                    continue;
                }
                if (message.msgType == MessageType.control) {
                    if (message.message.equals("MARKER")) {
                        ownNode.markerActions(connectedNode, message);
                    }
                    if (message.message.equals("TERMINATE")){
                        keepListening = false;
                        sc.close();
                        ownNode.terminate.set(true);
                        synchronized (ownNode) {
                            ownNode.notify();
                        }
                    }

                } else if (message.msgType == MessageType.application){
                    if (ownNode.redChannels.contains(connectedNode)) {
                        ownNode.addMsg(message);
                    }
                    int[] msgVectClock = message.vectorClock;
                    System.out.println("Received: " + message.message + " with vector clock: " + Arrays.toString(msgVectClock));
                    ownNode.syncSet(msgVectClock);
                    if (!ownNode.active.getAndSet(true)){
                        notifyNode();
                    }
                } else if (message.msgType == MessageType.state) {
                    System.out.println("Received state from node " + connectedNode + " about node " + ((NodeState) message.message).nodeId);
                    if (ownNode.getNodeId() != 0) {
                        SctpChannel parentSC = ownNode.getChannel(ownNode.treeParent);
                        message.send(parentSC);
                    } else {
                        synchronized (ownNode.nodeStateMap) {
                            ownNode.nodeStateMap.put(((NodeState) message.message).nodeId, (NodeState) message.message);
                            ownNode.processSnapshot();
                        }
                    }
                }

            }
            System.out.println("Received all messages");
        } catch (ClosedChannelException e){
            System.out.println("Received all messages");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
        System.out.println("Finished listening to node " + connectedNode);

        // Threads automatically terminate after finishing run
    }

    public void notifyNode() {
        synchronized(ownNode) {
            ownNode.notify();
        }
    }
}
