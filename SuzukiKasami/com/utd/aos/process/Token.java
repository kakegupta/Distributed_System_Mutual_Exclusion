package com.utd.aos.process;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Suzuki Kasami token. This has a queue and a list of requests
 * satisfied.
 */
public class Token implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private Queue<Integer> tokenQueue;
	private List<Integer> requestsSatisfied;
	
	public Token(int nProc) {
		tokenQueue = new LinkedList<Integer>();
		requestsSatisfied = new ArrayList<Integer>(nProc);
		for (int i = 0; i < nProc; i++) {
			requestsSatisfied.add(0);
		}
	}
	
	public Queue<Integer> getTokenQueue() {
		return tokenQueue;
	}
	
	public List<Integer> getRequestsSatisfied() {
		return requestsSatisfied;
	}
	
	public String toString() {
		return requestsSatisfied.toString() + " / " + tokenQueue.toString();
	}

}
