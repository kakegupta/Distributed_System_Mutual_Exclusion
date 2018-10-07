package com.utd.aos.process;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.utd.aos.connection.Connection;
import com.utd.aos.connection.ServerConnection;

/**
 * This is the process class for coordinator process. Implements specialized
 * methods for the Proc class for coordinator processes.
 *
 */
public class CoordinatorProc extends Proc {

	private int nPidsAssigned;
	
	/*
	 * Maps that store hostnames and connection objects corresponding
	 * to all the processes.
	 */
	private Map<Integer, ServerConnection> pidToConnMap;
	private Map<Integer, String> pidToHostnameMap;
	private Map<Integer, Integer> pidToHolderMap;
	
	private Set<Integer> completedProcs;
	
	private Map<Integer, ProcStats> procStats;
	private List<Map.Entry<Integer, Map.Entry<Instant, Instant>>> aggStats;
	private long totalSyncDelay;
	private long totalWaitTime;
	private int nTotalReceived;
	private int nTotalSent;
	
	public CoordinatorProc(int nProc, Map<Integer, List<Integer>> neighborMap, int t1, int t2, int t3) {
		super(nProc, neighborMap, t1, t2, t3);
	}
	
	@Override
	public void init(String coordinatorHostname) throws Exception {
		super.init(coordinatorHostname);
		pid = 1;
		nPidsAssigned = 0;
		initConn = new ServerConnection();
		pidToConnMap = new HashMap<Integer, ServerConnection>(nProc - 1);
		pidToHostnameMap = new HashMap<Integer, String>(nProc - 1);
		pidToHolderMap = new HashMap<Integer, Integer>(nProc - 1);
		completedProcs = new HashSet<Integer>(nProc - 1);
		procStats = new HashMap<Integer, ProcStats>();
		aggStats = new ArrayList<Map.Entry<Integer, Map.Entry<Instant, Instant>>>();
		totalSyncDelay = 0;
		totalWaitTime = 0;
		nTotalReceived = 0;
		nTotalSent = 0;
		populatePidToHolderMap();
		System.out.println(pidToHolderMap);
	}
	
	/**
	 * Runs BFS on the network graph to retrieve a tree out of
	 * the graph. Keeps initial holders for every node in a map.
	 */
	void populatePidToHolderMap() {
		Set<Integer> visited = new HashSet<Integer>();
		Queue<Integer> visitQueue = new LinkedList<Integer>();
		visitQueue.add(pid);
		visited.add(pid);
		while(!visitQueue.isEmpty()) {
			int nodePid = visitQueue.poll();
			List<Integer> neighborList = neighborMap.get(nodePid);
			for (int neighborPid : neighborList) {
				if (!visited.contains(neighborPid)) {
					pidToHolderMap.put(neighborPid, nodePid);
					visitQueue.add(neighborPid);
					visited.add(neighborPid);
				}
			}
		}
	}
	
	/**
	 * Opens a connection on port 20000 and keeps listening until
	 * nProc - 1 registering processes have connected with it and 
	 * received pids from it one at a time. Starts assigning pids
	 * from 2 as 1 is reserved for the coordinator itself.
	 */
	@Override
	public void assignPids() {
		Thread t = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while (nPidsAssigned < nProc -1) {
						initConn.connectRetry(coordinatorHostname, 20000);

						nPidsAssigned++;
						// Pid 1 is reserved for the coordinator.
						int newPid = nPidsAssigned + 1;
						String hostname = initConn.getHostName();
						initConn.send(((Integer)newPid).toString());
						initConn.send(pidToHolderMap.get(newPid).toString());
						synchronized (pidToHostnameMap) {
							pidToHostnameMap.put(newPid, hostname);
						}
						System.out.println("PID: " + newPid + " sent\n");

						initConn.close();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				System.out.println(pidToHostnameMap.toString());
			}
		});
		t.start();
	}
	
	/**
	 * Establishes connections with all the registering processes on
	 * different ports (one for each process). Port no for process
	 * 'i' would be 101'i'.
	 */
	@Override
	public void establishCoordinatorConn() throws InterruptedException {
		List<Thread> tList= new ArrayList<Thread>();
		for (int i = 0; i < nProc-1; i++) {
			int clientPid = i + 2;
			Thread t = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						ServerConnection conn = new ServerConnection();
						synchronized (pidToConnMap) {
							pidToConnMap.put(clientPid, conn);
						}
						int port = 20000 + (100 * 1) + (clientPid);
						conn.connectRetry(coordinatorHostname, port);
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
		System.out.println(pidToConnMap.toString());
	}
	
	/**
	 * Sends to all the registering processes the list of their neighbors
	 * using the connections established in establishCoordinatorConn().
	 */
	@Override
	public void communicateNeighborHostnames() throws InterruptedException {
		List<Thread> tList= new ArrayList<Thread>();
		for (int i = 0; i < nProc-1; i++) {
			int clientPid = i + 2;
			Thread t = new Thread(new Runnable() {
				@Override
				public void run() {
					Connection conn = pidToConnMap.get(clientPid);
					List<Integer> clientNeighborPids = neighborMap.get(clientPid);
					for (int clientNeighborPid : clientNeighborPids) {
						String neighborHostname;
						if (clientNeighborPid == 1)
							neighborHostname = coordinatorHostname;
						else
							neighborHostname = pidToHostnameMap.get(clientNeighborPid);
						
						try {
							conn.send(neighborHostname);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			});
			tList.add(t);
			t.start();
		}
		for (Thread t : tList) {
			t.join();
		}
	}

	/**
	 * Since the coordinator has connections established with all the
	 * other processes, it just needs to populate its neighborPidTo...Maps.
	 * Sends a 'HELLO' message to all its neighbors.
	 */
	@Override
	public void establishNeighborConn() throws Exception {
		List <Thread> tList = new ArrayList<Thread>();
		List<Integer> neighborPids = neighborMap.get(pid);
		nNeighbors = neighborPids.size();
		for (int neighborPid : neighborPids) {
			Thread t = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						String neighborHostname = pidToHostnameMap.get(neighborPid);
						Connection neighborConn = new ServerConnection();
						int port = 10000 + (100*pid) + neighborPid;
						neighborConn.connectRetry(coordinatorHostname, port);
						neighborPidToHostnameMap.put(neighborPid, neighborHostname);
						neighborPidToConnMap.put(neighborPid, neighborConn);
						sendQueues.put(neighborPid, new LinkedList<String>());
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
		System.out.println(neighborPidToHostnameMap);
		System.out.println(neighborPidToConnMap);
	}
	
	@Override
	public void start() throws Exception {
		super.start();
		for (Map.Entry<Integer, ServerConnection> entry : pidToConnMap.entrySet()) {
			StatsCollectorRun statsCollRun = new StatsCollectorRun(entry.getKey(), entry.getValue());
			Thread tStatsCollector = new Thread(statsCollRun);
			tStatsCollector.start();
		}
		handleCompleteAndStats();
	}
	
	/**
	 * Waits for all the processes to complete. Initiates terminate
	 * once all the processes are complete. Then waits for all the
	 * processes to send their stats. Aggregates and prints the
	 * stats.
	 * 
	 * @throws Exception
	 */
	private void handleCompleteAndStats() throws Exception {
		synchronized (completedProcs) {
			while (completedProcs.size() != nProc)
				completedProcs.wait();
		}
		initiateTerminate();
		
		synchronized (procStats) {
			while (procStats.size() != nProc)
				procStats.wait();
		}
		
		aggregateStats();
	}
	
	/**
	 * Initiates terminate by adding a terminate message to it's
	 * receive queue.
	 */
	private void initiateTerminate() {
		Map.Entry<Integer, String> pidMessagePair = 
				new AbstractMap.SimpleEntry<Integer, String>(0, TERMINATE_MESSAGE);
		synchronized (receiveQueue) {
			receiveQueue.add(pidMessagePair);
			receiveQueue.notifyAll();
		}
	}
	
	/**
	 * Aggregates and prints all the stats received from all the
	 * other processes. Sorts all the critical sections according to 
	 * their start times. Calculates the averages of sync delay, total
	 * wait time, and total messages sent and received.
	 */
	private void aggregateStats() {
		for (Map.Entry<Integer, ProcStats> entry : procStats.entrySet()) {
			for (Map.Entry<Instant, Instant> startEndPair : entry.getValue().csStartAndEndTime)
				aggStats.add(new HashMap.SimpleEntry(entry.getKey(), startEndPair));
			totalWaitTime += entry.getValue().waitTime;
			nTotalReceived += entry.getValue().nReceived;
			nTotalSent += entry.getValue().nSent;
		}
		Collections.sort(aggStats, new SortByStartTime());
		int nTotalCS = aggStats.size();
		System.out.println(String.format("Total no. of CSs executed : %d", nTotalCS));
		for (Map.Entry<Integer, Map.Entry<Instant, Instant>> entry : aggStats)
			System.out.println(entry);
		
		for (int i = 1; i < aggStats.size(); i++)
			totalSyncDelay += Duration.between(aggStats.get(i - 1).getValue().getValue(), aggStats.get(i).getValue().getKey()).toNanos();
		
		System.out.println(String.format("\nTotal synchronization delay in nanoseconds : %d", totalSyncDelay));
		System.out.println(String.format("Average synchronization delay in nanoseconds : %f\n", (double)totalSyncDelay/(double)(nTotalCS - 1)));
		
		System.out.println(String.format("\nTotal wait time in milliseconds : %d", totalWaitTime));
		System.out.println(String.format("Average wait time in milliseconds : %f\n", (double)totalWaitTime/(double)nTotalCS));
		
		System.out.println(String.format("Total messages received : %d", nTotalReceived));
		System.out.println(String.format("Average messages received : %f\n", (double)nTotalReceived/(double)nTotalCS));
		
		System.out.println(String.format("Total messages sent : %d", nTotalSent));
		System.out.println(String.format("Average messages sent : %f\n", (double)nTotalSent/(double)nTotalCS));
	}
	
	class SortByStartTime implements Comparator<Map.Entry<Integer, Map.Entry<Instant, Instant>>>
	{
	    public int compare(Map.Entry<Integer, Map.Entry<Instant, Instant>> a, Map.Entry<Integer, Map.Entry<Instant, Instant>> b)
	    {
	        return a.getValue().getKey().isAfter(b.getValue().getKey()) ? 1 : -1;
	    }
	}
	
	@Override
	public void sendCompleteAndStats() throws Exception {
		synchronized (completedProcs) {
			completedProcs.add(pid);
			completedProcs.notifyAll();
		}
		
		synchronized (completedProcs) {
			while (completedProcs.size() != nProc)
				completedProcs.wait();
		}

		System.out.println(String.format("No. of CSs executed by the process : %d", csStartAndEndTime.size()));
		System.out.println(String.format("Total wait time of the process : %d", waitTime));
		System.out.println(String.format("Messages received by the process : %d", nReceived));
		System.out.println(String.format("Messages sent by the process : %d\n", nSent));
		synchronized (procStats) {
			procStats.put(pid, new ProcStats(csStartAndEndTime, waitTime, nReceived, nSent));
			procStats.notifyAll();
		}
	}
	
	/**
	 * Closes all the connections and calls deinit of the superclass.
	 */
	@Override
	public void deinit() throws Exception{
		for (Map.Entry<Integer, ServerConnection> entry : pidToConnMap.entrySet()) {
			Connection conn = entry.getValue();
			conn.close();
		}
		super.deinit();
	}
	
	/**
	 * Stats collector class. An object is created for every
	 * node. Receives all the complete messages and stats of
	 * the process with which the object is associated.
	 */
	public class StatsCollectorRun implements Runnable {
		
		int partnerPid;
		Connection partnerConn;
		
		public StatsCollectorRun(int pid, Connection conn) {
			this.partnerPid = pid;
			this.partnerConn = conn;
		}
		
		/**
		 * Receives complete message from the associated process.
		 * Sends stats request to the process once the coordinator has
		 * received complete messages from all the processes. receives
		 * stats from the associated process.
		 */
		@Override
		public void run() {
			try {
				partnerConn.receive();
				String message = partnerConn.getConnBuffer();
				if (isCompleteMessage(message)) {
					synchronized (completedProcs) {
						completedProcs.add(partnerPid);
						completedProcs.notifyAll();
					}
				}
				synchronized (completedProcs) {
					while (completedProcs.size() != nProc)
						completedProcs.wait();
				}
				
				partnerConn.send("SEND_STATS");
				partnerConn.receiveObject();
				ProcStats partnerStats = (ProcStats)partnerConn.getConnObjBuffer();
				synchronized (procStats) {
					procStats.put(partnerPid, partnerStats);
					procStats.notifyAll();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}

}
