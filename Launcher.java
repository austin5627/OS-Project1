import com.sun.nio.sctp.MessageInfo;
import com.sun.nio.sctp.SctpChannel;
import com.sun.nio.sctp.SctpServerChannel;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class NodeConfig {
	public int id;
	public String ip;
	public int port;
	public ArrayList<Integer> neighbors;

	public NodeConfig(int id, String ip, int port) {
		this.id = id;
		this.ip = ip;
		this.port = port;
	}
}

// Just a random class so I could create the GitHub repo. Feel free to delete
public class Launcher {
	private static final int LAUNCHER_PORT = 15754;
	public static String nodeMap = "";

	public static void main(String[] args) {
		String filename = System.getenv("CONFIGLOCAL");
		File configFile = new File(filename);

		Pattern nodePattern = Pattern.compile("(\\d+) (dc\\d+) (\\d+)");
		Pattern neighborPattern = Pattern.compile("\\d+\\b");

		int numNodes = 0, minPerActive = 0, maxPerActive = 0, minSendDelay = 0, snapshotDelay = 0, maxNumber = 0;
		ArrayList<NodeConfig> nodes = new ArrayList<>();

		try {
			BufferedReader br = new BufferedReader(new FileReader(configFile));
			String line;

			int lineNumber = -1;
			// Loop over config file
			StringBuilder sb = new StringBuilder();
			while ((line = br.readLine()) != null) {
				if (line.trim().isEmpty() || line.trim().startsWith("#")) {
					continue;
				}
				lineNumber++;
				if (line.contains("#")) {
					line = line.substring(0, line.indexOf('#'));
				}

				if (lineNumber == 0) {
					Scanner scanner = new Scanner(line);
					numNodes = scanner.nextInt();
					minPerActive = scanner.nextInt();
					maxPerActive = scanner.nextInt();
					minSendDelay = scanner.nextInt();
					snapshotDelay = scanner.nextInt();
					maxNumber = scanner.nextInt();
				} else if (lineNumber <= numNodes){
					Matcher nodeMatcher = nodePattern.matcher(line);
					if (!nodeMatcher.find()){
						System.out.println("No matches");
						System.exit(0);
					}
					int nodeID = Integer.parseInt(nodeMatcher.group(1));
					String nodeHost = nodeMatcher.group(2);
					int nodePort = Integer.parseInt(nodeMatcher.group(3));
					nodes.add(new NodeConfig(nodeID, nodeHost, nodePort));
					nodeMap = sb.append(nodeID).append(" ").append(nodeHost).append(" ").append(nodePort).append("\n").toString();
				} else if (lineNumber <= 2*numNodes) {
					Matcher neighborMatcher = neighborPattern.matcher(line);
					ArrayList<Integer> neighbors = new ArrayList<>();
					for (int i = 1; i < neighborMatcher.groupCount(); i++) {
						neighbors.add(Integer.parseInt(neighborMatcher.group(i)));
					}
					nodes.get(lineNumber-numNodes-1).neighbors = neighbors;
				} else {
					System.err.println("On line " + lineNumber + " when only " + numNodes*2 + " should exist\n" + line);
				}
			}
			br.close();
		} catch (IOException e) {
			System.out.println(configFile.getPath());
			System.out.println("Couldn't read from file");
			e.printStackTrace();
			System.exit(0);
		}

		try {
			Runtime run = Runtime.getRuntime();
			run.exec("ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $netid@$host " +
					"java -cp $BINDIR $PROG; exec bash");
		}
		catch (IOException e){
			e.printStackTrace();
			System.exit(0);
		}
		try {
			startNode(nodes, nodeMap, minPerActive, maxPerActive, minSendDelay, snapshotDelay, maxNumber);
		}
		catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	public static void startNode(ArrayList<NodeConfig> nodes, String nodeMap, int minPerActive, int maxPerActive, int minSendDelay, int snapshotDelay, int maxNumber) throws Exception {
		InetSocketAddress addr = new InetSocketAddress(LAUNCHER_PORT); // Get address from port number
		for (NodeConfig nc : nodes) {
			SctpServerChannel ssc = SctpServerChannel.open(); //Open server channel
			ssc.bind(addr);//Bind server channel to address

			SctpChannel sc = ssc.accept();

			MessageInfo messageInfo = MessageInfo.createOutgoing(null, 0); // MessageInfo for SCTP layer

			// Global Parameters
			Message msg = new Message(String.valueOf(minPerActive));
			sc.send(msg.toByteBuffer(), messageInfo);
			msg = new Message(String.valueOf(maxPerActive));
			sc.send(msg.toByteBuffer(), messageInfo);
			msg = new Message(String.valueOf(minSendDelay));
			sc.send(msg.toByteBuffer(), messageInfo);
			msg = new Message(String.valueOf(snapshotDelay));
			sc.send(msg.toByteBuffer(), messageInfo);
			msg = new Message(String.valueOf(maxNumber));
			sc.send(msg.toByteBuffer(), messageInfo);

			// Node Info
			msg = new Message(String.valueOf(nc.id)); //Node ID
			sc.send(msg.toByteBuffer(), messageInfo);
			msg = new Message(String.valueOf(nc.ip)); // Node IP
			sc.send(msg.toByteBuffer(), messageInfo);
			msg = new Message(String.valueOf(nc.port)); // Node Port
			sc.send(msg.toByteBuffer(), messageInfo);

			// Neighbor node info
			StringBuilder neighborMap = new StringBuilder();
			for (String n : nodeMap.split("\n")){
				int nID = Integer.parseInt(n.replaceAll(" .*", ""));
				if (nc.neighbors.contains(nID)){
					neighborMap.append("\n").append(n);
				}
			}
			msg = new Message(neighborMap.toString());
			sc.send(msg.toByteBuffer(), messageInfo);
		}
	}
}
