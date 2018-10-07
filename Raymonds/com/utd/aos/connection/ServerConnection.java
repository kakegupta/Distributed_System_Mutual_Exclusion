package com.utd.aos.connection;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

public class ServerConnection extends Connection {

	ServerSocket serverSocket;
	
	public ServerConnection() throws IOException {
		super();
		serverSocket = new ServerSocket();
	}
	
	/**
	 * This method exposes a connection for a client to connect.
	 * The connection is accepted at the port provided in the
	 * parameter.
	 * 
	 * @param hostname of the machine calling this method.
	 * @param port on which the connection is to be opened.
	 * @throws IOException
	 */
	private void connect(String hostname, int port) throws IOException {
		if (serverSocket.isClosed())
			serverSocket = new ServerSocket();

		serverSocket.bind(new InetSocketAddress(hostname, port));
		connSocket = serverSocket.accept();
		objOutputStream = new ObjectOutputStream(connSocket.getOutputStream());
		//objOutputStream.flush();
		objInputStream = new ObjectInputStream(connSocket.getInputStream());
	}
	
	@Override
	public void connectRetry(String hostname, int port) throws IOException {
		if (serverSocket.isBound())
			this.close();

		System.out.println("Accepting connection on port: " + port + '\n');
		connect(hostname, port);
		System.out.println("Connection established: " + this + '\n');
	}
	
	@Override
	public void close() throws IOException {
		super.close();
		serverSocket.close();
		System.out.println("Connection closed\n");
	}
	
}
