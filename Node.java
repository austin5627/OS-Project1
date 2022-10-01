import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.sun.nio.sctp.MessageInfo;
import com.sun.nio.sctp.SctpChannel;

import java.net.InetSocketAddress;
import java.util.concurrent.CopyOnWriteArraySet;
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

class NodeState implements Serializable {
    List<Integer> vectorClock;
    boolean active;
    Set<Message> inTransitMsgs;
    int nodeId;

    public NodeState(int nodeId, List<Integer> vectorClock, boolean active, Set<Message> inTransitMsgs) {
        this.vectorClock = vectorClock;
        this.active = active;
        this.inTransitMsgs = inTransitMsgs;
        this.nodeId = nodeId;
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
    private final AtomicBoolean startSnapshot = new AtomicBoolean(false);
    public final AtomicBoolean endSnapshot = new AtomicBoolean(true);
    private final AtomicBoolean startConvergeCast = new AtomicBoolean(false);
    public final AtomicBoolean terminate = new AtomicBoolean(false);
    private final ConcurrentHashMap<Integer, SctpChannel> channelMap = new ConcurrentHashMap<>();
    private List<Integer> snapshot;
    private final Set<Message> inTransitMsgs;
    public final Map<Integer, NodeState> nodeStateMap;

    public int treeParent;
    public final CopyOnWriteArraySet<Integer> redChannels;

    public static final int MAX_MSG_SIZE = 4096;
    private static String filename;


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
        this.vectClock = Collections.synchronizedList(new ArrayList<>(Collections.nCopies(numNodes, 0)));
        redChannels = new CopyOnWriteArraySet<>();
        inTransitMsgs = new HashSet<>();
        nodeStateMap = new ConcurrentHashMap<>();
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
            filename = (String) Message.receiveMessage(sc).message;

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

            if (node.nodeID == 0) {
                for (int i = 0; i < numNodes; i++) {
                    File file = new File(filename + "-" + i + ".out");
                    file.delete();
                }
            }

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

    public void startProtocol() {
        this.createConnections();
        while (!allConnectionsEstablished.get()) {
            waitSynchronized();
        }
        if (node.nodeID == 0) {
            SnapshotThread st = new SnapshotThread(snapShotDelay, node);
            st.start();
        }
        while (sentMessages < maxNumber) {
            while (!active.get() && !startSnapshot.get() && !terminate.get() && !startConvergeCast.get()) {
                this.waitSynchronized();
            }
            if (terminate.get()) {
                terminateProtocol();
                return;
            }
            else if (startConvergeCast.get()) {
                convergeCast();
                continue;
            }
            else if (startSnapshot.get()) {
                takeSnapshot();
                continue;
            }
            Object[] neighborMapKeys = neighborMap.keySet().toArray();
            Random random = new Random();
            int numMsgs = random.nextInt(maxPerActive - minPerActive + 1) + minPerActive;
            long waitStart = 0;
            while (sentMessages < numMsgs) {

                // Wait minSendDelay to send next message
                waitSynchronized(minSendDelay);
                if (startSnapshot.get()) {
                    takeSnapshot();
                    if (System.currentTimeMillis() - waitStart < minSendDelay) {
                        continue;
                    }
                }
                else if (startConvergeCast.get()) {
                    convergeCast();
                    if (System.currentTimeMillis() - waitStart < minSendDelay) {
                        continue;
                    }
                }
                int neighborIndex = (int) neighborMapKeys[random.nextInt(neighborMapKeys.length)];
                try {
                    SctpChannel channel = channelMap.get(neighborIndex);
                    syncSend(channel, "Hi from node " + nodeID);
                    sentMessages++;
                    waitStart = System.currentTimeMillis();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(0);
                }

            }
            active.set(false);
        }

    }

    private void terminateProtocol() {
        System.out.println("Terminating");
        for (SctpChannel channel : channelMap.values()) {
            try {
                if (channel.isOpen()) {
                    Message msg = new Message(nodeID, MessageType.control, "TERMINATE");
                    msg.send(channel);
                    channel.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("Halting node " + nodeID);
        System.exit(0);
    }


    public void takeSnapshot() {
        System.out.println("Taking snapshot");
        snapshot = new ArrayList<>();
        snapshot.addAll(vectClock);
        nodeStateMap.clear();
        for (SctpChannel channel: channelMap.values()) {
            Message snapshotMsg = new Message(nodeID, MessageType.control, "MARKER");
            snapshotMsg.send(channel);
        }
        startSnapshot.set(false);
        endSnapshot.set(false);
    }

    public void convergeCast() {
        System.out.println("Starting converge cast");
        NodeState state = new NodeState(nodeID, snapshot, active.get(), inTransitMsgs);
        Message stateMsg = new Message(nodeID, MessageType.state, state);
        stateMsg.send(channelMap.get(treeParent));
        startConvergeCast.set(false);
    }

    public void processSnapshot() {
        System.out.println("Processing snapshot");
        if (nodeStateMap.size() < numNodes - 1) {
            return;
        }
        nodeStateMap.put(nodeID, new NodeState(nodeID, snapshot, active.get(), inTransitMsgs));
        // do stuff
        boolean allPassive = true;
        boolean messagesInTransit = false;
        Map<Integer, List<Integer>> vcMap = new HashMap<>();
        for (NodeState state : nodeStateMap.values()) {
            if (state.active) {
                allPassive = false;
            }
            if (state.inTransitMsgs.size() > 0) {
                messagesInTransit = true;
            }
            int i = state.nodeId;
            List<Integer> clock = state.vectorClock;
            vcMap.put(i, clock);
            String outputFileName = filename + "-" + i + ".out";
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName, true))){
                for (int j : clock) {
                    writer.write(j + " ");
                }
                writer.write("\n");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println("Consistency: " + isConsistent(vcMap));

        if (allPassive && !messagesInTransit) {
            // Terminate all connections
            terminateProtocol();
        } else {
            endSnapshot.set(true);
        }
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
                    String msg_content = "Hi to Node " + i + " from Node " + nodeID;
                    syncSend(sc, msg_content);
                    ListenerThread listenerThread = new ListenerThread(this, sc, i);
                    listenerThread.start();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(0);
                }

            }
        }
    }



    public boolean isConsistent(Map<Integer, List<Integer>> vcMap) {
        for (int i : vcMap.keySet()) {
            for (int j : vcMap.keySet()) {
                if (vcMap.get(i).get(i) < vcMap.get(j).get(i)) {
                    return false;
                }
            }
        }
        return true;
    }

    public void markerActions(int connectedNode, Message msg) {
        synchronized (LOCK) {
            synchronized (redChannels) {
                // Need to synchronize this check so multiple channels can't think they are the first to receive a marker
                if (redChannels.isEmpty()) {
                    System.out.println("First marker received");
                    treeParent = connectedNode;
                    redChannels.addAll(neighborMap.keySet());
                    redChannels.remove(connectedNode);
                    startSnapshot.set(true);
                    synchronized (this){
                        this.notify();
                    }
                }
                else {
                    redChannels.remove(connectedNode);
                }
                // Outside the else for the case that a node only has one connection
                if (redChannels.isEmpty()) {
                    System.out.println("All markers received");
                    if (nodeID != 0) {
                        startConvergeCast.set(true);
                        synchronized (this){
                            this.notify();
                        }
                    }
                }
            }
        }
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

    public void syncSet(int[] msgVectClock) {
        synchronized (LOCK) {
            synchronized (vectClock) {
                for (int i = 0; i < vectClock.size(); i++) {
                    if (vectClock.get(i) < msgVectClock[i]) {
                        vectClock.set(i, msgVectClock[i]);
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

    public void addMsg(Message msg) {
        inTransitMsgs.add(msg);
    }

    public void syncSend(SctpChannel sc, String message_content) throws Exception{
        synchronized (LOCK) {
            synchronized (vectClock) {
                syncIncr();
                Message msg = new Message(nodeID, MessageType.application, message_content, vectClock);
                msg.send(sc);
                System.out.println("\t" + message_content + " Vector Clock: " + node.vectClock.toString());

            }
        }
    }

}
