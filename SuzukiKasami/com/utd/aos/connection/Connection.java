package com.utd.aos.connection;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketAddress;

public abstract class Connection {
	
	public final static String HOSTNAME_SEPARATOR = "/";
	final static String PORT_SEPARATOR = ":";
	
	Socket connSocket;
	ObjectInputStream objInputStream;
	ObjectOutputStream objOutputStream;
	Object connObjBuffer;
	
	public Connection() {
		connObjBuffer = new Object();
	}
	
	/**
	 * This method tries to establish the connection with the remote machine.
	 * In case an exception is thrown while trying to establish the connection,
	 * the method will try to connect again for a given number of times.
	 * 
	 * @param hostname of the server machine
	 * @param port of the server machine on which the socket is exposed
	 * @throws Exception
	 */
	public abstract void connectRetry(String hostname, int port) throws Exception;

	/**
	 * Writes the message to the output stream of the connection.
	 * 
	 * @param message
	 * @throws IOException
	 */
	public void send(String message) throws IOException {
		objOutputStream.writeObject(message);
//		System.out.println("Sent to " + this + " : " + message + '\n');
	}

	/**
	 * Writes the message object to the output stream of the connection.
	 * 
	 * @param message object to be sent
	 * @throws IOException
	 */
	public void sendObject(Object message) throws IOException {
		objOutputStream.writeObject(message);
//		System.out.println("Sent to " + this + " : " + message + '\n');
	}
	
	/**
	 * Receives the message from the connection reader object
	 * and writes it into the object buffer of the connection.
	 * 
	 * @throws IOException
	 * @throws ClassNotFoundException 
	 */
	public void receive() throws IOException, ClassNotFoundException {
		connObjBuffer = objInputStream.readObject();
//		System.out.println("Received from " + this + " : " + getConnBuffer() + '\n');
	}
	
	/**
	 * Receives the message from the connection reader object
	 * and writes it into the object buffer of the connection.
	 * 
	 * @throws IOException
	 * @throws ClassNotFoundException 
	 */
	public void receiveObject() throws IOException, ClassNotFoundException {
		connObjBuffer = objInputStream.readObject();
//		System.out.println("Received from " + this + " : " + getConnBuffer() + '\n');
	}
	
	/**
	 * Method to retrieve the string from the object buffer of the connection.
	 * 
	 * @return the connBuffer member object of the class.
	 */
	public String getConnBuffer() {
		return (String)connObjBuffer;
	}
	
	/**
	 * Method to retrieve the object buffer of the connection.
	 * 
	 * @return the connBuffer member object of the class.
	 */
	public Object getConnObjBuffer() {
		return connObjBuffer;
	}
	
	/**
	 * Method that closes the connection.
	 * Closes the Socket object of the connection.
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException {
		connSocket.close();
		System.out.println("Socket closed\n");
	}

	/**
	 * This method returns the SocketAddress of the remote machine
	 * with which this connection is associated.
	 * 
	 * @return the SocketAddress of the remote machine
	 */
	public SocketAddress getRemoteSocketAddress() {
		return connSocket.getRemoteSocketAddress();
	}

	/**
	 * Retrieves the hostname of the remote machine with which the connection
	 * is associated from the SocketAddress of the remote machine.
	 * 
	 * @return the hostname of the remote machine. Returns the IP address if
	 *         the hostname is not available in the SocketAddress object.
	 */
	public String getHostName() {
		String sockAddStr = this.getRemoteSocketAddress().toString();
		int hostnameSeparatorIndex = sockAddStr.lastIndexOf(HOSTNAME_SEPARATOR);
		int portSeparatorIndex = sockAddStr.lastIndexOf(PORT_SEPARATOR);
		String hostname;
		if (hostnameSeparatorIndex == 0)
			hostname = sockAddStr.substring(hostnameSeparatorIndex+1, portSeparatorIndex);
		else
			hostname = sockAddStr.substring(0, hostnameSeparatorIndex);
		return hostname;
	}
	
	/**
	 * Returns the string form of the Socket object of the Connection.
	 */
	public String toString() {
		return connSocket.toString();
	}
	
}
