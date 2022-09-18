import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import com.sun.nio.sctp.SctpChannel;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.lang.System;
import java.util.Random;

class NodeInfo {
    String ip;
    int port;

    public NodeInfo(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }
}

public class Node extends Thread {
    private static Node node;
    private int minPerActive, maxPerActive, minSendDelay, snapShotDelay, maxNumber;
    private int nodeID;
    private String ip;
    private int port;
    private Map<Integer, NodeInfo> neighborMap;

    private AtomicInteger numFinishedListening;
    private AtomicBoolean allConnectionsEstablished;
    private ConcurrentHashMap<Integer, SctpChannel> channelMap;
    private ConcurrentHashMap<Integer, AcceptThread> acceptThreadMap;
    private ConcurrentHashMap<Integer, ListenerThread> listenerThreadMap;

    public static final int MAX_MSG_SIZE = 4096;



    public Node() {
        numFinishedListening = new AtomicInteger(0);
        allConnectionsEstablished = new AtomicBoolean(false);

        // The "dcXX" servers start at dc01, so adding one makes
        // the node number the same as the machine number
        channelMap = new ConcurrentHashMap<>();
        acceptThreadMap = new ConcurrentHashMap<>();
        listenerThreadMap = new ConcurrentHashMap<>();
    }

    public Node(int minPerActive, int maxPerActive, int minSendDelay, int snapShotDelay, int maxNumber, int nodeID,
                String ip, int port, Map<Integer, NodeInfo> neighborMap) {
        this.minPerActive = minPerActive;
        this.maxPerActive = maxPerActive;
        this.minSendDelay = minSendDelay;
        this.snapShotDelay = snapShotDelay;
        this.maxNumber = maxNumber;
        this.nodeID = nodeID;
        this.ip = ip;
        this.port = port;
        this.neighborMap = neighborMap;
    }

    public static void main(String[] args) throws Exception {
        String launcherIP = args[0];
        System.out.println("Started");
        int launcherPort = Integer.parseInt(args[1]);
        InetSocketAddress addr = new InetSocketAddress(launcherIP, launcherPort);
        SctpChannel sc;
        sc = SctpChannel.open(addr, 0, 0);

        receiveConfig(sc);
        node.toString();
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
                ",\nacceptThreadMap=" + acceptThreadMap +
                ",\nlistenerThreadMap=" + listenerThreadMap +
                "\n}";
    }

    public static void receiveConfig(SctpChannel sc) {
        try {
            ByteBuffer buf = ByteBuffer.allocateDirect(Node.MAX_MSG_SIZE); // Messages are received over SCTP using ByteBuffer

            // Global Parameters
            sc.receive(buf, null, null);
            int minPerActive = Message.fromByteBuffer(buf).toInt();
            sc.receive(buf, null, null);
            int maxPerActive = Message.fromByteBuffer(buf).toInt();
            sc.receive(buf, null, null);
            int minSendDelay = Message.fromByteBuffer(buf).toInt();
            sc.receive(buf, null, null);
            int snapshotDelay = Message.fromByteBuffer(buf).toInt();
            sc.receive(buf, null, null);
            int maxNumber = Message.fromByteBuffer(buf).toInt();

            // Node info about self
            sc.receive(buf, null, null);
            int id = Message.fromByteBuffer(buf).toInt();
            sc.receive(buf, null, null);
            String ip = Message.fromByteBuffer(buf).message;
            sc.receive(buf, null, null);
            int port = Message.fromByteBuffer(buf).toInt();
            sc.receive(buf, null, null);

            // Neighbor node info
            sc.receive(buf, null, null);
            String nodesInfoString = Message.fromByteBuffer(buf).message;
            String mapEntry;
            Scanner scanner = new Scanner(nodesInfoString);
            Map<Integer, NodeInfo> neighborMap = new HashMap<>();
            while(scanner.hasNextLine()) {
                mapEntry = scanner.nextLine();
                System.out.println("Map Entry: " + mapEntry);
                Scanner intScanner = new Scanner(mapEntry);
                int neighborID = intScanner.nextInt();
                String neighborIP = intScanner.next();
                int neighborPort = intScanner.nextInt();
                NodeInfo neighborInfo = new NodeInfo(neighborIP, neighborPort);
                neighborMap.put(neighborID, neighborInfo);
            }

            node = new Node(minPerActive, maxPerActive, minSendDelay, snapshotDelay, maxNumber, id, ip, port, neighborMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void initializeConnections() {
        acceptConnections();
        // Give all the nodes time to get ready to accept
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        createConnections();

        // wait for all connections to be established
        while (channelMap.size() < 7) {
            try {
                synchronized(this) {
                    wait();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("Failed to wait");
            }
        }

        System.out.println("Fully connected");
    }

    public void acceptConnections() {
        // A node will accept connections from other nodes with a lower number
        // A node will try to connect to nodes with a higher number
        for (int i = 1; i < neighborMap.size(); i++) {
            AcceptThread acceptThread = new AcceptThread(this, (port / 10) + i);
            acceptThreadMap.put(i, acceptThread);
            acceptThread.start();
        }
    }

    public void createConnections() {
        // FIXME: This should be changed
        for (int i = nodeID + 1; i <= 8; i++) {
            if (i != nodeID) {
                ListenerThread listenerThread = new ListenerThread(this, i);
                listenerThreadMap.put(i, listenerThread);
                listenerThread.start();
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

    public int getPort() {
        return port;
    }

    public String getIp() {
        return ip;
    }

    public int getNodeId() {
        return nodeID;
    }

    public void addChannel(int connectedNode, SctpChannel sctpChannel) {
        channelMap.put(connectedNode, sctpChannel);
    }

    public SctpChannel getChannel(int i) {
        return channelMap.get(i);
    }

    public boolean containsChannel(int i) {
        return channelMap.containsKey(i);
    }

    public void finishedListening() {
        numFinishedListening.incrementAndGet();
    }
}
