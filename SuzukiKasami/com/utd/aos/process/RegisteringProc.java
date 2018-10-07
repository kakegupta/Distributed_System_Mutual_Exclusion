package com.utd.aos.process;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.utd.aos.connection.ClientConnection;
import com.utd.aos.connection.Connection;
import com.utd.aos.connection.ServerConnection;

/**
 * This is the process class for registering processes. Implements specialized
 * methods for the Proc class for registering processes.
 *
 */
public class RegisteringProc extends Proc {

	public RegisteringProc(int nProc, Map<Integer, List<Integer>> neighborMap, int t1, int t2, int t3) {
		super(nProc, neighborMap, t1, t2, t3);
	}
	
	@Override
	public void init(String coordinatorHostname) throws Exception {
		super.init(coordinatorHostname);
		initConn = new ClientConnection();
	}
	
	/**
	 * Connects with the coordinator on port no 20000.
	 * The coordinator sends a pid to the calling process.
	 */
	@Override
	public void assignPids() throws Exception {
		initConn.connectRetry(coordinatorHostname, 20000);
		
		initConn.receive();
		pid = Integer.parseInt(initConn.getConnBuffer());
		System.out.println("PID: " + pid + " received\n");
		
		initConn.close();
	}
	
	/**
	 * Establishes a connection with the coordinator
	 * on a port that is specific for this process. 101'pid'
	 */
	@Override
	public void establishCoordinatorConn() {
		try {
			int port = 20000 + (100 * 1) + (pid);
			initConn.connectRetry(coordinatorHostname, port);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Receives neighbor hostnames for the process sent to it
	 * by the coordinator on the coordinator connection. Puts
	 * the neighbor hostnames in a Map.
	 */
	@Override
	public void communicateNeighborHostnames() throws Exception {
		List<Integer> neighborPids = neighborMap.get(pid);
		nNeighbors = neighborPids.size();
		for (int neighborPid : neighborPids) {
			initConn.receive();
			String neighborHostname = initConn.getConnBuffer();
			System.out.println(neighborHostname);
			neighborPidToHostnameMap.put(neighborPid, neighborHostname);
			sendQueues.put(neighborPid, new LinkedList<Object>());
		}
		System.out.println(neighborPidToHostnameMap.toString());
	}

	/**
	 * Creates a connection for each of the process's neighbor. Sends
	 * a 'HELLO' message once the connection is established with a 
	 * neighbor. Opens a server connection if the pid of the calling
	 * process is less that the neighbor process's pid. Port on which 
	 * it opens the connection is 1'pid''neighborPid'. Opens a client
	 * connection otherwise and tries to connect on port 1'neighborPid''pid'.
	 */
	@Override
	public void establishNeighborConn() throws Exception {
		List<Thread> tList= new ArrayList<Thread>();
		for (Map.Entry<Integer, String> entry : neighborPidToHostnameMap.entrySet()) {
			int neighborPid = entry.getKey();
			String neighborHostname = entry.getValue();
			Thread t = new Thread(new Runnable() {
				@Override
				public void run() {
					int port;
					Connection neighborConn;
					try {
						if (neighborPid > pid) {
							neighborConn = new ServerConnection();
							port = 10000 + (100*pid) + neighborPid;
							// Fetch the hostname of the machine to open a server connection.
							String localHostname = InetAddress.getLocalHost().toString();
							localHostname = localHostname.substring(localHostname.indexOf(Connection.HOSTNAME_SEPARATOR) + 1);
							neighborConn.connectRetry(localHostname, port);
						} else {
							neighborConn = new ClientConnection();
							port = 10000 + (100*neighborPid) + pid;
							neighborConn.connectRetry(neighborHostname, port);
						}
						synchronized (neighborPidToConnMap) {
							neighborPidToConnMap.put(neighborPid, neighborConn);
						}
						// Send a 'HELLO' message
						sayHelloToNeighbor(neighborConn);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			tList.add(t);
			t.start();
		}
		for (Thread t : tList) {
			t.join();
		}
		System.out.println(neighborPidToConnMap.toString());
	}
	
	@Override
	public void sendCompleteAndStats() throws Exception {
		initConn.send(COMPLETE_MESSAGE);
		initConn.receive();
		String message = initConn.getConnBuffer();
		if (message.equals("SEND_STATS")) {
			initConn.sendObject(new ProcStats(csStartAndEndTime, waitTime, nReceived, nSent));
		}
		System.out.println(String.format("No. of CSs executed by the process : %d", csStartAndEndTime.size()));
		System.out.println(String.format("Total wait time of the process : %d", waitTime));
		System.out.println(String.format("Messages received by the process : %d", nReceived));
		System.out.println(String.format("Messages sent by the process : %d\n", nSent));
	}
	
}
