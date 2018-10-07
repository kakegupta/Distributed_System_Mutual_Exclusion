package com.utd.aos.process;

import java.io.IOException;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import com.utd.aos.connection.Connection;

/**
 * This is the class for the processors stages. This implements all the
 * stages of a process like initializing, establishing connections with
 * coordinator and neighbors, start execution, and deinit
 *
 */
public abstract class Proc {

	protected static final String TERMINATE_MESSAGE = "TERMINATE";

	protected static final String COMPLETE_MESSAGE = "COMPLETE";

	protected int pid;
	
	// No. of total processes
	protected int nProc;
	
	// Connection used to initialize and fetch pids from coordinator.
	protected Connection initConn;
	protected String coordinatorHostname;
	
	// Map that contains neighbor information of all the processes' pids.
	Map<Integer, List<Integer>> neighborMap;
	
	// No. of neighbors the process has.
	protected int nNeighbors;
	
	protected int holder;
	
	private int t1, t2, t3;
	
	/*
	 * Maps that store the hostname and connection objects corresponding to the
	 * neighbor pids.
	 */
	protected Map<Integer, Connection> neighborPidToConnMap;
	protected Map<Integer, String> neighborPidToHostnameMap;
	
	/* 
	 * Messages from all the neighbors go into this queue along 
	 * with the pid of the process who sent the message.
	 */
	protected LinkedList<Map.Entry<Integer, String>> receiveQueue;
	
	/* 
	 * This is a map of neighbor pids to their respective send queues. 
	 * Messages meant for a neighbor are added to the neighbor's queue.
	 */
	protected Map<Integer, LinkedList<String>> sendQueues;

	protected List<Map.Entry<Instant, Instant>> csStartAndEndTime;
	protected long waitTime;
	protected int nReceived;
	protected int nSent;
	
	public Proc(int nProc, Map<Integer, List<Integer>> neighborMap, int t1, int t2, int t3) {
		this.nProc = nProc;
		this.neighborMap = neighborMap;
		this.t1 = t1;
		this.t2 = t2;
		this.t3 = t3;
	}
	
	/**
	 * Initializes the member variables of the class.
	 * 
	 * @param coordinatorHostname hostname of the coordinator process.
	 * @throws Exception
	 */
	public void init(String coordinatorHostname) throws Exception {
		this.coordinatorHostname = coordinatorHostname;
		holder = 0;
		receiveQueue = new LinkedList<Map.Entry<Integer, String>>();
		sendQueues = new HashMap<Integer, LinkedList<String>>();
		neighborPidToConnMap = new HashMap<Integer, Connection>(5);
		neighborPidToHostnameMap = new HashMap<Integer, String>(5);
		this.csStartAndEndTime = new ArrayList<Map.Entry<Instant, Instant>>();
		this.waitTime = 0;
		this.nReceived = 0;
		this.nSent = 0;
	}
	
	public abstract void assignPids() throws Exception;

	public abstract void establishCoordinatorConn() throws Exception;
	
	public abstract void communicateNeighborHostnames() throws Exception;
	
	public abstract void establishNeighborConn() throws Exception;
	
	/**
	 * 
	 * @return the pid of the current process.
	 */
	public int getPid() {
		return pid;
	}
	
	/**
	 * Sends a 'HELLO' message to the connection passed to the method.
	 * Receives a 'HELLO' message from the connection passed to the method.
	 * 
	 * @param neighborConn connection object of one of the neighbors.
	 * @throws Exception
	 */
	protected void sayHelloToNeighbor(Connection neighborConn) throws Exception {
		Thread tSend = new Thread(new Runnable() {
			@Override
			public void run() {
				sendHelloMessage(neighborConn);
			}
		});
		tSend.start();
		Thread tReceive = new Thread(new Runnable() {
			@Override
			public void run() {
				receiveHelloMessage(neighborConn);
			}
		});
		tReceive.start();
		
		tSend.join();
		tReceive.join();
	}

	/**
	 * Receives a message from the connection passed to it and
	 * confirms that it's a 'HELLO' message.
	 * 
	 * @param neighborConn
	 */
	private void receiveHelloMessage(Connection neighborConn) {
		try {
			neighborConn.receive();
			if (!neighborConn.getConnBuffer().equals("HELLO"))
				throw new Exception("Should receive a HELLO message");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Sends 'HELLO' to the connection passed to it.
	 * 
	 * @param neighborConn
	 */
	private void sendHelloMessage(Connection neighborConn) {
		try {
			neighborConn.send("HELLO");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Closes the init connection and all the neighbor connections of the process.
	 * 
	 * @throws Exception
	 */
	public void deinit() throws Exception {
		initConn.close();
		for (Map.Entry<Integer, Connection> entry : neighborPidToConnMap.entrySet()) {
			Connection conn = entry.getValue();
			conn.close();
		}
	}

	/**
	 * Instantiates an executor for the process. Along with the executor,
	 * multiple communicators are instantiated, one for each neighbor.
	 * The communicators start listening for any messages and the executor
	 * waits for new messages to compute. 
	 */
	public void start() throws Exception{
		if (nNeighbors == 0)
			return;
		
		Executor executor = new Executor(receiveQueue, sendQueues);
		
		MessageHandlerRunnable msgHdlrRun = new MessageHandlerRunnable(executor);
		Thread tMsgHdlr = new Thread(msgHdlrRun);
		tMsgHdlr.start();
		
		RequestHandlerRunnable reqHdlrRun = new RequestHandlerRunnable(executor);
		Thread tReqHdlr = new Thread(reqHdlrRun);
		tReqHdlr.start();
		
		CSExecutorRunnable csExecRun = new CSExecutorRunnable(executor);
		Thread tCsExec = new Thread(csExecRun);
		tCsExec.start();
		
		for (Map.Entry<Integer, Connection> entry : neighborPidToConnMap.entrySet()) {
			Communicator comm = new Communicator(entry.getKey(), entry.getValue(), receiveQueue, sendQueues.get(entry.getKey()));
			SenderRunnable sendRun = new SenderRunnable(comm);
			ReceiverRunnable receiveRun = new ReceiverRunnable(comm);
			Thread tSend = new Thread(sendRun);
			Thread tReceive = new Thread(receiveRun);
			tSend.start();
			tReceive.start();
		}
	}
	
	/**
	 * Sends complete message to the coordinator after executing all
	 * the critical sections. Sends stats to the coordinator once it receives
	 * a request from the coordinator.
	 * 
	 * @throws Exception
	 */
	abstract protected void sendCompleteAndStats() throws Exception;
	
	/**
	 * Checks whether the message starts with 'TERMINATE'.
	 * 
	 * @param message
	 * @return true if the message starts with 'TERMINATE'
	 */
	private boolean isTerminateMessage(String message) {
		if (message.startsWith(TERMINATE_MESSAGE))
			return true;
		else
			return false;
	}
	
	/**
	 * Checks whether the message starts with 'COMPLETE'.
	 * 
	 * @param message
	 * @return true if the message starts with 'COMPLETE'
	 */
	protected boolean isCompleteMessage(String message) {
		if (message.startsWith(COMPLETE_MESSAGE))
			return true;
		else
			return false;
	}
	
	/**
	 * Handles computations of request and token messages.
	 * Handles requests and executes critical sections. Only one
	 * instance is created for a process.
	 *
	 */
	class Executor {
		
		private static final String REQUEST_CS_MESSAGE = "REQUEST_CS";

		private static final String TOKEN_MESSAGE = "TOKEN";
		
		private final Object lock = new Object();
		
		private final Object lockCS = new Object();

		LinkedList<Map.Entry<Integer, String>> receiveQueue;
		Map<Integer, LinkedList<String>> sendQueues;
		Random rand;
		Queue<Integer> requestCSQueue;
		boolean shouldEnterCS;
		
		public Executor(LinkedList<Map.Entry<Integer, String>> receiveQueue, Map<Integer, LinkedList<String>> sendQueues) {
			this.receiveQueue = receiveQueue;
			this.sendQueues = sendQueues;
			this.rand = new Random(System.currentTimeMillis()/pid);
			this.requestCSQueue = new LinkedList<Integer>();
			this.shouldEnterCS = false;
		}
		
		/**
		 * Checks whether the message starts with 'REQUEST_CS'.
		 * 
		 * @param message
		 * @return true if the message starts with 'REQUEST_CS'
		 */
		private boolean isRequestCSMessage(String message) {
			if (message.startsWith(REQUEST_CS_MESSAGE))
				return true;
			else
				return false;
		}
		
		/**
		 * Checks whether the message starts with 'TOKEN'.
		 * 
		 * @param message
		 * @return true if the message starts with 'TOKEN'
		 */
		private boolean isToken(String message) {
			if (message.startsWith(TOKEN_MESSAGE))
				return true;
			else
				return false;
		}
		
		/**
		 * Executes on the next message from the receive queue.
		 * 
		 * @throws Exception
		 */
		private void handleMessages() throws Exception {
			while (true) {
				Map.Entry<Integer, String> pidMessagePair;
				synchronized (receiveQueue) {
					while (receiveQueue.isEmpty()) {
						receiveQueue.wait();
					}
					pidMessagePair = receiveQueue.remove();
				}
				int senderPid = pidMessagePair.getKey();
				String message = pidMessagePair.getValue();
				System.out.println(String.format("%s : received from process : %d at time : %s\n", message, senderPid, Instant.now()));
				if (isTerminateMessage(message)) {
					handleTerminate();
					break;
				} else if (isRequestCSMessage(message)) {
					nReceived++;
					addRequest(senderPid);
				} else if (isToken(message)) {
					nReceived++;
					handleToken();
				}
			}
		}
		
		/**
		 * Handles the terminate message. Adds 0 to the request queue
		 * to indicate that the execution has terminated.
		 */
		private void handleTerminate() {
			synchronized (lock) {
				requestCSQueue.add(0);
				makeRoot();
				lock.notifyAll();
			}
			sendTerminateToAll();
		}
		
		/**
		 * Waits till the process receives the token and becomes the root.
		 * Executes critical section or sends token to the next process in the
		 * queue based on the queue head value. Terminates if queue has 0 at head.
		 * 
		 * @throws Exception
		 */
		private void handleRequests() throws Exception {
			while (true) {
				int requesterPid;
				synchronized (lock) {
					while (requestCSQueue.isEmpty() || !isRoot()) {
						lock.wait();
					}
					System.out.println(requestCSQueue + "\n");
					requesterPid = requestCSQueue.poll();
				}
				if (requesterPid == 0) {
					break;
				} else if (requesterPid == getPid()) {
					execAndWaitForCS();
				} else {
					execRequest(requesterPid);
				}
			}
		}
		
		/**
		 * Adds the request to its request queue if the process
		 * is the root. Sends the request to the parent if the request
		 * queue is empty and its not the root.
		 * 
		 * @param requesterPid
		 */
		private void addRequest(int requesterPid) {
			synchronized (lock) {
				if (requestCSQueue.isEmpty() && !isRoot()) {
					sendRequest(holder);
				}
				requestCSQueue.add(requesterPid);
				lock.notifyAll();
			}
		}
		
		/**
		 * Sends the token to the requester process. modify the holder
		 * to point to the requester process. Sends request to the parent
		 * if the request queue of the process is not empty.
		 * 
		 * @param requesterPid
		 */
		private void execRequest(int requesterPid) {
			synchronized (lock) {
				sendToken(requesterPid);
				modifyHolder(requesterPid);
				if (!requestCSQueue.isEmpty()) {
					sendRequest(holder);
				}
			}
		}
		
		private void modifyHolder(int newHolder) {
			holder = newHolder;
		}
		
		private boolean isRoot() {
			return holder == 0;
		}
		
		private void makeRoot() {
			modifyHolder(0);
		}
		
		private void handleToken() {
			synchronized (lock) {
				makeRoot();
				lock.notifyAll();
			}
		}
		
		/**
		 * Signals the critical section thread to enter and execute
		 * the critical section. Waits till the critical section is
		 * executed.
		 * 
		 * @throws Exception
		 */
		private void execAndWaitForCS() throws Exception {
			synchronized (lockCS) {
				shouldEnterCS = true;
				lockCS.notifyAll();
			}
			
			synchronized (lockCS) {
				while (shouldEnterCS)
					lockCS.wait();
			}
		}
		
		/**
		 * Waits for a given time interval. Then, requests for the
		 * critical section. Once the request is fulfilled, it executes the
		 * critical section. Repeats the process over 20 to 40 times. Once
		 * all the critical sections have been executed, sends complete and
		 * stats messages to the coordinator.
		 * 
		 * @throws Exception
		 */
		private void requestAndExecuteCS() throws Exception{
			int maxNCS = 40;
			int minNCS = 20;
			int nCS = rand.nextInt(maxNCS - (minNCS - 1)) + minNCS;
			System.out.println(String.format("No of critical sections to be executed : %d\n", nCS));
			for (int iCS = 0; iCS < nCS; iCS++) {
				int csWaitTime = rand.nextInt(t2 - (t1 - 1)) + t1;
				Thread.sleep(csWaitTime);
				addRequest(pid);
				long timeSubmit = System.currentTimeMillis();
				synchronized (lockCS) {
					while (!shouldEnterCS)
						lockCS.wait();
				}
				long timeFulfil = System.currentTimeMillis();
				waitTime += (timeFulfil - timeSubmit);
				enterCS(iCS);
				synchronized (lockCS) {
					shouldEnterCS = false;
					lockCS.notifyAll();
				}
			}
			sendCompleteAndStats();
		}
		
		/**
		 * Waits for a given interval. Notes the start and end time
		 * for the critical section.
		 * 
		 * @param iCS
		 * @throws Exception
		 */
		private void enterCS(int iCS) throws Exception{
			Instant startTime = Instant.now();
			System.out.println(String.format("\t\tProcess has entered CS no. %d at : %s", iCS, startTime));
			Thread.sleep(t3);
			Instant endTime = Instant.now();
			System.out.println(String.format("\t\tProcess has exited CS no. %d at : %s\n", iCS, endTime));
			Map.Entry<Instant, Instant> startEndPair = 
					new AbstractMap.SimpleEntry<Instant, Instant>(startTime, endTime);
			csStartAndEndTime.add(startEndPair);
		}
		
		private void sendRequest(int neighborPid) {
			nSent++;
			addToSendQueue(neighborPid, REQUEST_CS_MESSAGE);
		}
		
		private void sendToken(int neighborPid) {
			nSent++;
			addToSendQueue(neighborPid, TOKEN_MESSAGE);
		}
		
		/**
		 * Sends a 'TERMINATE' message to all the neighbors of the process.
		 */
		private void sendTerminateToAll() {
			for (int neighborPid : neighborMap.get(pid)) {
				addToSendQueue(neighborPid, TERMINATE_MESSAGE);
			}
		}
		
		/**
		 * Add the message to the send queue of the neighbor.
		 * 
		 * @param neighborPid pid of the neighbor to whom the message is to be sent
		 * @param message
		 */
		private void addToSendQueue(int neighborPid, String message) {
			Queue<String> sendQueue = sendQueues.get(neighborPid);
			synchronized (sendQueue) {
				sendQueue.add(message);
				sendQueue.notifyAll();
			}
			System.out.println(String.format("\t%s : sent to process : %d at time : %s\n", message, neighborPid, Instant.now()));
		}
		
	}
	
	/**
	 * Handles sends and receives for a neighbor. One instance is created for
	 * each neighbor.
	 *
	 */
	class Communicator {

		Queue<Map.Entry<Integer, String>> receiveQueue;
		Queue<String> sendQueue;
		Connection conn;
		int partnerPid;
		
		public Communicator(int partnerPid, Connection conn, 
				Queue<Map.Entry<Integer, String>> receiveQueue, Queue<String> sendQueue) {
			this.partnerPid = partnerPid;
			this.conn = conn;
			this.receiveQueue = receiveQueue;
			this.sendQueue = sendQueue;
		}
		
		/**
		 * Receives messages from the connection object and adds it to the
		 * receiveQueue. Terminates if a 'TERMINATE' message is received.
		 * 
		 * @throws Exception
		 */
		private void receive() throws Exception {
			while (true) {
				conn.receive();
				String message = conn.getConnBuffer();
				Map.Entry<Integer, String> pidMessagePair = 
						new AbstractMap.SimpleEntry<Integer, String>(partnerPid, message);
				synchronized (receiveQueue) {
					receiveQueue.add(pidMessagePair);
					receiveQueue.notifyAll();
				}
				if (isTerminateMessage(message))
					break;
			}
		}
		
		/**
		 * Takes messages from the send queue for the neighbor (partnerPid) and sends the
		 * message to the neighbor.
		 * 
		 * @throws Exception
		 */
		private void send() throws Exception {
			while (true) {
				String message;
				synchronized (sendQueue) {
					while (sendQueue.isEmpty()) {
						sendQueue.wait();
					}
					message = sendQueue.remove();
				}
				conn.send(message);
				if (isTerminateMessage(message))
					break;
			}
		}
		
	}
	
	/**
	 * Runnable class to run the execute method of the Executor.
	 * 
	 */
	class CSExecutorRunnable implements Runnable {

		Executor exec;
		
		public CSExecutorRunnable(Executor exec) {
			this.exec = exec;
		}
		
		@Override
		public void run() {
			try {
				exec.requestAndExecuteCS();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * Runnable class to run the execute method of the Executor.
	 * 
	 */
	class MessageHandlerRunnable implements Runnable {
		
		Executor exec;
		
		public MessageHandlerRunnable(Executor exec) {
			this.exec = exec;
		}
		
		@Override
		public void run() {
			try {
				exec.handleMessages();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	class RequestHandlerRunnable implements Runnable {
		
		Executor exec;
		
		public RequestHandlerRunnable(Executor exec) {
			this.exec = exec;
		}
		
		@Override
		public void run() {
			try {
				exec.handleRequests();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Runnable class to run the send method of the Communicator.
	 *
	 */
	class SenderRunnable implements Runnable {
		
		Communicator comm;
		
		public SenderRunnable(Communicator comm) {
			this.comm = comm;
		}
		
		@Override
		public void run() {
			try {
				comm.send();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * Runnable class to run the receive method of the Communicator.
	 * 
	 */
	class ReceiverRunnable implements Runnable {
		
		Communicator comm;
		
		public ReceiverRunnable(Communicator comm) {
			this.comm = comm;
		}
		
		@Override
		public void run() {
			try {
				comm.receive();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}

}
