package com.utd.aos.dproc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import com.utd.aos.process.CoordinatorProc;
import com.utd.aos.process.Proc;
import com.utd.aos.process.RegisteringProc;

public class Dproc {
	
	final static String COORDINATOR_ARG = "-c";

	public static void main(String[] args) {
		
		int nProcs;
		String coordinatorHostname;
		Map<Integer, List<Integer>> neighborMap;
		
		int t1;
		int t2;
		int t3;
		
		/*
		 * Read from dsConfig file and initialize the parameters for the process.
		 */
		try {
			BufferedReader fileReader = new BufferedReader(new FileReader("dsConfig"));
			String line = fileReader.readLine();
			coordinatorHostname = line.substring(12);					// length of "COORDINATOR " = 12
			line = fileReader.readLine();
			nProcs = Integer.parseInt(line.substring(20));				// length of "NUMBER OF PROCESSES " = 20
			line = fileReader.readLine();
			t1 = Integer.parseInt(line.substring(3));					// length of "T1 " = 3
			line = fileReader.readLine();
			t2 = Integer.parseInt(line.substring(3));					// length of "T2 " = 3
			line = fileReader.readLine();
			t3 = Integer.parseInt(line.substring(3));					// length of "T3 " = 3
			line = fileReader.readLine();								// Skip this line
			
			/*
			 * Read the neighbor map.
			 */
			neighborMap = new HashMap<Integer, List<Integer>>(nProcs);
			for (int i = 0; i < nProcs; i++) {
				line = fileReader.readLine();
				StringTokenizer lineToken = new StringTokenizer(line);
				int pid = Integer.parseInt(lineToken.nextToken());
				List<Integer> neighborList = new ArrayList<Integer>();
				while (lineToken.hasMoreTokens())
					neighborList.add(Integer.parseInt(lineToken.nextToken()));
				neighborMap.put(pid, neighborList);
			}
			fileReader.close();

			Proc proc;

			/*
			 * Instantiate a process based on the '-c' flag.
			 */
			if (args.length == 1 && args[0].equals(COORDINATOR_ARG)) {
//				proc = new CoordinatorProcSingleThread(nProcs, neighborMap, t1, t2, t3);
				proc = new CoordinatorProc(nProcs, neighborMap, t1, t2, t3);
			} else {
				proc = new RegisteringProc(nProcs, neighborMap, t1, t2, t3);
			}

			/*
			 * Go through all the process stages.
			 */
			proc.init(coordinatorHostname);
			proc.assignPids();
			proc.establishCoordinatorConn();
			proc.communicateNeighborHostnames();
			proc.establishNeighborConn();
			proc.start();
			while (Thread.activeCount() > 1) {
				Thread.sleep(1000);
			}
			proc.deinit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
