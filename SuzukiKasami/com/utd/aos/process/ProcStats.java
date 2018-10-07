package com.utd.aos.process;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Stores the start and end times of all the critical sections
 * for a process. Along with that, stores the total wait time, and 
 * the total no of messages sent and received.
 */
public class ProcStats implements Serializable {
	
	private static final long serialVersionUID = 8400207246982289933L;
	public List<Map.Entry<Instant, Instant>> csStartAndEndTime;
	public long waitTime;
	public int nReceived;
	public int nSent;
	
	public ProcStats(List<Map.Entry<Instant, Instant>> csStartAndEndTime, long waitTime,
			int nReceived, int nSent) {
		this.csStartAndEndTime = csStartAndEndTime;
		this.waitTime = waitTime;
		this.nReceived = nReceived;
		this.nSent = nSent;
	}
}
