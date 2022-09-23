import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.sun.nio.sctp.MessageInfo;
import com.sun.nio.sctp.SctpChannel;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.lang.System;

class NodeInfo {
    String ip;
    int port;
    InetSocketAddress addr;

    public NodeInfo(String ip, int port) {
        this.ip = ip;
        this.port = port;
        this.addr = new InetSocketAddress(ip, port);
    }

    @Override
    public String toString() {
        return "ip: " + ip + "\tport: " + port;
    }
}

public class Node extends Thread {
    private static Node node;
    private final int minPerActive, maxPerActive, minSendDelay, snapShotDelay, maxNumber;
    private final int nodeID;
    private final String ip;
    private final int port;
    private final Map<Integer, NodeInfo> neighborMap;

    private final int numNodes;
    public AtomicBoolean active;

    private int sentMessages;
    private final Object LOCK = new Object();
    List<Integer> vectClock;

    private final AtomicInteger numFinishedListening = new AtomicInteger(0);
    private final AtomicBoolean allConnectionsEstablished = new AtomicBoolean(false);
    private final ConcurrentHashMap<Integer, SctpChannel> channelMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer, List<Integer>> vcMap;

    public static final int MAX_MSG_SIZE = 4096;


    public Node(int minPerActive, int maxPerActive, int minSendDelay, int snapShotDelay, int maxNumber, int nodeID,
                String ip, int port, int numNodes, Map<Integer, NodeInfo> neighborMap) {
        this.minPerActive = minPerActive;
        this.maxPerActive = maxPerActive;
        this.minSendDelay = minSendDelay;
        this.snapShotDelay = snapShotDelay;
        this.maxNumber = maxNumber;
        this.nodeID = nodeID;
        this.ip = ip;
        this.port = port;
        this.numNodes = numNodes;
        this.neighborMap = neighborMap;
        this.active = new AtomicBoolean(nodeID == 0);
        List<Integer> initialClock = new ArrayList<>(Collections.nCopies(numNodes, 0));
        // Give all the vector clocks values to make isConsistent return false if they are never updated
        vcMap = new ConcurrentHashMap<>();
        for (int i = 0; i < numNodes; i++) {
            vcMap.put(i, initialClock);
        }
        initialClock.set(nodeID, 1);
        this.vectClock = Collections.synchronizedList(initialClock);
    }

    public static void main(String[] args) throws Exception {
        String launcherIP = args[0];
        System.out.println("Started");
        int launcherPort = Integer.parseInt(args[1]);
        InetSocketAddress addr = new InetSocketAddress(launcherIP, launcherPort);
        SctpChannel sc;
        sc = SctpChannel.open(addr, 0, 0);

        receiveConfig(sc);
        System.out.println(node.toString());
    }

    @Override
    public String toString() {
        return "Node{" +
                "\nminPerActive=" + minPerActive +
                ",\nmaxPerActive=" + maxPerActive +
                ",\nminSendDelay=" + minSendDelay +
                ",\nsnapShotDelay=" + snapShotDelay +
                ",\nmaxNumber=" + maxNumber +
                ",\nid=" + nodeID +
                ",\nip='" + ip + '\'' +
                ",\nport=" + port +
                ",\nneighborMap=" + neighborMap +
                ",\nnumFinishedListening=" + numFinishedListening +
                ",\nallConnectionsEstablished=" + allConnectionsEstablished +
                ",\nchannelMap=" + channelMap +
                "\n}";
    }

    public static void receiveConfig(SctpChannel sc) {
        try {
            // Global Parameters
            int minPerActive = (int) Message.receiveMessage(sc).message;
            int maxPerActive = (int) Message.receiveMessage(sc).message;
            int minSendDelay = (int) Message.receiveMessage(sc).message;
            int snapshotDelay = (int) Message.receiveMessage(sc).message;
            int maxNumber = (int) Message.receiveMessage(sc).message;

            // Node info about self
            int id = (int) Message.receiveMessage(sc).message;
            String ip = (String) Message.receiveMessage(sc).message;
            int port = (int) Message.receiveMessage(sc).message;


            // Other node info
            int numNodes = (int) Message.receiveMessage(sc).message;
            String nodesInfoString = (String) Message.receiveMessage(sc).message;
            String mapEntry;
            Scanner scanner = new Scanner(nodesInfoString);
            Map<Integer, NodeInfo> neighborMap = new HashMap<>();
            while(scanner.hasNextLine()) {
                mapEntry = scanner.nextLine();
                if (mapEntry.isEmpty()) {
                    continue;
                }
                Scanner intScanner = new Scanner(mapEntry);
                int neighborID = intScanner.nextInt();
                String neighborIP = intScanner.next();
                int neighborPort = intScanner.nextInt();
                NodeInfo neighborInfo = new NodeInfo(neighborIP, neighborPort);
                neighborMap.put(neighborID, neighborInfo);
            }

            node = new Node(minPerActive, maxPerActive, minSendDelay, snapshotDelay, maxNumber, id, ip, port, numNodes, neighborMap);
            // Let Launcher know that it is accepting connections
            AcceptThread ac = new AcceptThread(node, node.port);
            ac.start();
            System.out.println("AC started");
            Message msg = new Message("Initialized");
            msg.send(sc);
            System.out.println("Send initialized");

            if (!Message.receiveMessage(sc).message.equals("Start Connections")){
                System.err.println("Didn't receive start message");
            }
            System.out.println("STARTING NODE " + node.nodeID);

            node.startProtocol();

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public void startProtocol(){
        this.createConnections();
        while (sentMessages < maxNumber) {
            while (!allConnectionsEstablished.get() || !active.get()) {
                this.waitSynchronized();
            }
            Object[] neighborMapKeys = neighborMap.keySet().toArray();
            Random random = new Random();
            int numMsgs = random.nextInt(maxPerActive - minPerActive + 1) + minPerActive;
            while (sentMessages < numMsgs) {

                int neighborIndex = (int) neighborMapKeys[random.nextInt(neighborMapKeys.length)];
                NodeInfo nextNeighbor = neighborMap.get(neighborIndex);
                MessageInfo messageInfo = MessageInfo.createOutgoing(null, 0);
                try {
                    SctpChannel channel = channelMap.get(neighborIndex);
                    syncSend(channel, "Hi from node" + nodeID);
                    System.out.println("Sent message to " + neighborIndex);
                    sentMessages++;

                    // Wait minSendDelay to send next message
                    waitSynchronized(minSendDelay);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(0);
                }

            }
            active.set(false);
        }

    }


    public void takeSnapshot() {
        /*
        part 2 goes here
         */

        boolean isConsistent = isConsistent();
    }

    public void waitSynchronized() {
        synchronized (this){
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(0);
            }
        }
    }

    public void waitSynchronized(int minSendDelay) {
        synchronized (this){
            try {
                wait(minSendDelay);
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(0);
            }
        }
    }


    public void createConnections() {

        for (int i : neighborMap.keySet()) {
            if (i < nodeID) {
                NodeInfo neighbor = neighborMap.get(i);
                SctpChannel sc;
                // A node will accept connections from other nodes with a lower number
                // A node will try to connect to nodes with a higher number
                try {
                    sc = SctpChannel.open(neighbor.addr, 0, 0);
                    this.addChannel(i, sc); // Connect to server using the address
                    MessageInfo messageInfo = MessageInfo.createOutgoing(null, 0); // MessageInfo for SCTP layer
                    String msg_content = "Hi from Node " + nodeID;
                    syncSend(sc, msg_content);
                    System.out.println("\t Message sent to node " + i + ": " + msg_content);
                    ListenerThread listenerThread = new ListenerThread(this, sc, i);
                    listenerThread.start();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(0);
                }

            }
        }
    }

    public boolean isConsistent() {
        synchronized (LOCK) {
            synchronized (vcMap) {
                for (int i : vcMap.keySet()) {
                    for (int j : vcMap.keySet()) {
                        if (vcMap.get(i).get(i) <= vcMap.get(j).get(i)) {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    public int getPort() {
        return port;
    }

    public String getIp() {
        return ip;
    }

    public int getNodeId() {
        return nodeID;
    }

    public Map<Integer, NodeInfo> getNeighborMap() {
        return neighborMap;
    }

    public void addChannel(int connectedNode, SctpChannel sctpChannel) {
        System.out.println("Adding connection to " + connectedNode);
        channelMap.put(connectedNode, sctpChannel);
        if (channelMap.size() == neighborMap.size()) {
            allConnectionsEstablished.set(true);
            synchronized (this){
                this.notify();
            }
        }
    }

    public SctpChannel getChannel(int i) {
        return channelMap.get(i);
    }

    public boolean containsChannel(int i) {
        return channelMap.containsKey(i);
    }

    public boolean getAllConnectionsEstablished(){
        return allConnectionsEstablished.get();
    }

    public void syncIncr() {
        synchronized (LOCK) {
            synchronized (vectClock) {
                vectClock.set(nodeID, vectClock.get(nodeID) + 1);
            }
        }
    }

    public void syncSet(List<Integer> msgVectClock) {
        synchronized (LOCK) {
            synchronized (vectClock) {
                for (int i = 0; i < vectClock.size(); i++) {
                    if (vectClock.get(i) < msgVectClock.get(i)) {
                        vectClock.set(i, msgVectClock.get(i));
                    }
                }
                syncIncr();
            }
        }
    }

    public int syncGet(int i) {
        synchronized (LOCK) {
            synchronized (vectClock) {
                return vectClock.get(i);
            }
        }
    }

    public void syncSend(SctpChannel sc, String message_content) throws Exception{
        synchronized (LOCK) {
            synchronized (vectClock) {
                syncIncr();
                Message msg = new Message(MessageType.string, message_content, vectClock);
                msg.send(sc);
            }
        }
    }


    /*
    public void sendIntegers() {
        Random rand = new Random();
        int broadcastInt;
        MessageInfo messageInfo = MessageInfo.createOutgoing(null, 0); // MessageInfo for SCTP layer
        Message msg;
        for (int i = 0; i < 50; i++) {
            try {
                Thread.sleep(rand.nextInt(17) + 2);
                broadcastInt = rand.nextInt(1001);
                broadcastSum += broadcastInt;
                for (SctpChannel sc : channelMap.values()) {
                    msg = new Message(Integer.toString(broadcastInt));
                    sc.send(msg.toByteBuffer(), messageInfo);
                }
            } catch (InterruptedException e) {
                System.out.println("daskjgfgkjffal");
            } catch (IOException e) {
                System.out.println("grsa");
            } catch (Exception e) {
                System.out.println("ewhg");
            }
        }
    }
     */

    /*
    public void report() {
        System.out.println("Sum for self: " + broadcastSum);

        while (numFinishedListening.get() < 7) {
            try {
                synchronized(this) {
                    wait(1000);
                }
            } catch (InterruptedException e) {
                System.out.println(e);
            }
            System.out.println("NumFinishedListening:" + numFinishedListening.get());
        }

        for (Integer i : sumMap.keySet()) {
            System.out.println("Sum for node " + i + ": " + sumMap.get(i));
        }
    }
     */

}
