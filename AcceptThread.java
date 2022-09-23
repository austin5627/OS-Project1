/*
 * Ethan Cooper
 * ewc180001
 * CS 6378.001
 */
import com.sun.nio.sctp.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class AcceptThread extends Thread {
    private final Node ownNode;
    private final int portNum;
    private boolean acceptNew;

    public AcceptThread(Node ownNode, int portNum) {
        this.ownNode = ownNode;
        this.portNum = portNum;
        this.acceptNew = true;
    }

    public void run() {
        try {
            InetSocketAddress addr = new InetSocketAddress(portNum); // Get address from port number
            SctpServerChannel ssc = SctpServerChannel.open(); //Open server channel
            ssc.bind(addr);//Bind server channel to address
            while (acceptNew) {
                SctpChannel sc = ssc.accept();
                // Should get a message immediately from client with the nodeNum of the remote device
                Message message = Message.receiveMessage(sc);
                String msg = (String) message.message;
                int connectedNodeNum = message.sender;
                System.out.println("Message received from node " + connectedNodeNum + ": " + msg);

                // parse int from the message
                ListenerThread lt = new ListenerThread(ownNode, sc, connectedNodeNum);
                lt.start();
                ownNode.addChannel(connectedNodeNum, sc);
                if (ownNode.getAllConnectionsEstablished()) {
                    acceptNew = false;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

    }
}