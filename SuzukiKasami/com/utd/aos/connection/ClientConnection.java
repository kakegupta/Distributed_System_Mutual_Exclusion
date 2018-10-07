package com.utd.aos.connection;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

public class ClientConnection extends Connection {
	
	final int nRetry = 200;
	
	public ClientConnection() {
		super();
		connSocket = new Socket();
	}

	/**
	 * This method tries to connect with the remote server machine
	 * identified by the given hostname and the port.
	 * 
	 * @param hostname remote server machine's hostname
	 * @param port remote server machine's port no.
	 * @throws IOException
	 */
	private void connect(String hostname, int port) throws IOException {
		try {
			if (connSocket.isClosed()) {
				System.out.println("Connection is closed: creating socket");
				connSocket = new Socket();
			}
			System.out.println("Requesting connection with: " + hostname + ':' + port + '\n');
			connSocket.connect(new InetSocketAddress(hostname, port));
			objOutputStream = new ObjectOutputStream(connSocket.getOutputStream());
			//objOutputStream.flush();
			objInputStream = new ObjectInputStream(connSocket.getInputStream());
			System.out.println("Connection established with: " + hostname + ':' + port + '\n');
		}
		catch (IOException e) {
			System.out.println(e.toString());
			connSocket.close();
		}
	}
	
	/**
	 * This method will try to connect to the remote machine nRetry times.
	 * It waits for a second before retrying.
	 */
	@Override
	public void connectRetry(String hostname, int port) throws IOException, InterruptedException {
		if (!connSocket.isClosed())
			this.close();
		
		for (int i = 0; i < nRetry; i++) {
			connect(hostname, port);
			if (connSocket.isConnected() && !connSocket.isClosed()) {
				break;
			}
			Thread.sleep(1000);
		}
		
		if (!connSocket.isConnected()) {
			connSocket.close();
			throw new IOException("Not able to connect to the host server");
		}
	}

}
