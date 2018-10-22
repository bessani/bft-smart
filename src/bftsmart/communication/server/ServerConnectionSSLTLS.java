/**
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bftsmart.communication.server;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.crypto.SecretKey;
import javax.net.ssl.HandshakeCompletedEvent;
import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.communication.SystemMessage;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.reconfiguration.VMMessage;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.util.TOMUtil;

/**
 * This class represents a connection with other server.
 *
 * ServerConnections are created by ServerCommunicationLayer.
 *
 * @author alysson
 */
public class ServerConnectionSSLTLS {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	private static final long POOL_TIME = 4000;
	private ServerViewController controller;
	private SSLSocket socketSSL;
	private DataOutputStream socketOutStream = null;
	private DataInputStream socketInStream = null;
	private int remoteId;
	private boolean useSenderThread;
	protected LinkedBlockingQueue<byte[]> outQueue;// = new LinkedBlockingQueue<byte[]>(SEND_QUEUE_SIZE);
	private HashSet<Integer> noMACs = null; // this is used to keep track of data to be sent without a MAC.
											// It uses the reference id for that same data
	private LinkedBlockingQueue<SystemMessage> inQueue;


	private Lock connectLock = new ReentrantLock();
	/** Only used when there is no sender Thread */
	private Lock sendLock;
	private boolean doWork = true;

	String[] ciphers = new String[] { "TLS_RSA_WITH_NULL_SHA256", "TLS_ECDHE_ECDSA_WITH_NULL_SHA",
			"TLS_ECDHE_RSA_WITH_NULL_SHA", "SSL_RSA_WITH_NULL_SHA", "TLS_ECDH_ECDSA_WITH_NULL_SHA",
			"TLS_ECDH_RSA_WITH_NULL_SHA", "TLS_ECDH_anon_WITH_NULL_SHA", "SSL_RSA_WITH_NULL_MD5" };

	public ServerConnectionSSLTLS(ServerViewController controller, SSLSocket socketSSL, int remoteId,
			LinkedBlockingQueue<SystemMessage> inQueue, ServiceReplica replica) {

		this.controller = controller;

		this.socketSSL = socketSSL;

		this.remoteId = remoteId;

		this.inQueue = inQueue;

		this.outQueue = new LinkedBlockingQueue<byte[]>(this.controller.getStaticConf().getOutQueueSize());

		this.noMACs = new HashSet<Integer>();
		// Connect to the remote process or just wait for the connection?
		if (isToConnect()) {

			// I have to connect to the remote server
			try {
				// SSL Socket.
				SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
				this.socketSSL = (SSLSocket) factory.createSocket(this.controller.getStaticConf().getHost(remoteId),
						this.controller.getStaticConf().getServerToServerPort(remoteId));

				this.socketSSL.setEnabledCipherSuites(ciphers);

				this.socketSSL.addHandshakeCompletedListener(new HandshakeCompletedListener() {
					@Override
					public void handshakeCompleted(HandshakeCompletedEvent event) {
						logger.debug("SSL/TLS handshake complete (ServerConnectionSSLTLS)!, Id:{}"
								+ "  ## CipherSuite: {}, ", remoteId, event.getCipherSuite());
					}
				});

				this.socketSSL.startHandshake();

				ServersCommunicationLayerSSLTLS.setSSLSocketOptions(this.socketSSL);
				new DataOutputStream(this.socketSSL.getOutputStream())
						.writeInt(this.controller.getStaticConf().getProcessId());

			} catch (UnknownHostException ex) {
				logger.error("Failed to connect to replica. Possible reason, replica is offline.");
				// logger.error("Failed to connect to replica", ex);
			} catch (IOException ex) {
				logger.error("Failed to connect to replica", ex);
			}
		}
		// else I have to wait a connection from the remote server

		if (this.socketSSL != null) {
			try {
				socketOutStream = new DataOutputStream(this.socketSSL.getOutputStream());
				socketInStream = new DataInputStream(this.socketSSL.getInputStream());
			} catch (IOException ex) {
				logger.error("Error creating connection to " + remoteId, ex);
			}
		}

		// ******* EDUARDO BEGIN **************//
		this.useSenderThread = this.controller.getStaticConf().isUseSenderThread();

		if (useSenderThread && (this.controller.getStaticConf().getTTPId() != remoteId)) {
			new SenderThread().start();
		} else {
			sendLock = new ReentrantLock();
		}
		// authenticateAndEstablishAuthKey();

		if (!this.controller.getStaticConf().isTheTTP()) {
			if (this.controller.getStaticConf().getTTPId() == remoteId) {
				// Uma thread "diferente" para as msgs recebidas da TTP
				new TTPReceiverThread(replica).start();
			} else {
				new ReceiverThread().start();
			}
		}
		// ******* EDUARDO END **************//
	}

	public SecretKey getSecretKey() {
		// return authKey;
		/*
		 * This is used to insert a proof at Acceptor (insertProof()) but, is it
		 * necessary when using SSL/TLS
		 */
		return null;
	}

	/**
	 * Stop message sending and reception.
	 */
	public void shutdown() {
		logger.debug("SHUTDOWN for " + remoteId);

		doWork = false;
		closeSocket();
	}

	/**
	 * Used to send packets to the remote server.
	 */
	public final void send(byte[] data, boolean useMAC) throws InterruptedException {
		if (useSenderThread) {
			// only enqueue messages if there queue is not full
			if (!useMAC) {
				logger.debug("Not sending defaultMAC " + System.identityHashCode(data));
				noMACs.add(System.identityHashCode(data));
			}

			if (!outQueue.offer(data)) {
				logger.debug("Out queue for " + remoteId + " full (message discarded).");
			}
		} else {
			sendLock.lock();
			sendBytes(data, useMAC);
			sendLock.unlock();
		}
	}

	/**
	 * try to send a message through the socket if some problem is detected, a
	 * reconnection is done
	 */
	private final void sendBytes(byte[] messageData, boolean useMAC) {
		boolean abort = false;
		do {
			if (abort)
				return; // if there is a need to reconnect, abort this method
			if (socketSSL != null && socketOutStream != null) {
				try {
					// do an extra copy of the data to be sent, but on a single out stream write
					byte[] data = new byte[5 + messageData.length];// without MAC
					int value = messageData.length;

					System.arraycopy(new byte[] { 
												(byte) (value >>> 24), 
												(byte) (value >>> 16), 
												(byte) (value >>> 8),
												(byte) value }, 0, data, 0, 4);
					System.arraycopy(messageData, 0, data, 4, messageData.length);
					System.arraycopy(new byte[] { (byte) 0 }, 0, data, 4 + messageData.length, 1);

					socketOutStream.write(data);

					return;
				} catch (IOException ex) {
					closeSocket();
					waitAndConnect();
					abort = true;
				}
			} else {
				waitAndConnect();
				abort = true;
			}
		} while (doWork);
	}

	// ******* EDUARDO BEGIN **************//
	// return true of a process shall connect to the remote process, false otherwise
	private boolean isToConnect() {
		if (this.controller.getStaticConf().getTTPId() == remoteId) {
			// Need to wait for the connection request from the TTP, do not tray to connect
			// to it
			return false;
		} else if (this.controller.getStaticConf().getTTPId() == this.controller.getStaticConf().getProcessId()) {
			// If this is a TTP, one must connect to the remote process
			return true;
		}
		boolean ret = false;
		if (this.controller.isInCurrentView()) {

			// in this case, the node with higher ID starts the connection
			if (this.controller.getStaticConf().getProcessId() > remoteId) {
				ret = true;
			}

		}
		return ret;
	}
	// ******* EDUARDO END **************//

	/**
	 * (Re-)establish connection between peers.
	 *
	 * @param newSocket
	 *            socket created when this server accepted the connection (only used
	 *            if processId is less than remoteId)
	 */
	protected void reconnect(SSLSocket newSocket) {

		connectLock.lock();

		if (socketSSL == null || !socketSSL.isConnected()) {

			try {

				// ******* EDUARDO BEGIN **************//
				if (isToConnect()) {

					SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
					this.socketSSL = (SSLSocket) factory.createSocket(this.controller.getStaticConf().getHost(remoteId),
							this.controller.getStaticConf().getServerToServerPort(remoteId));

					this.socketSSL.setEnabledCipherSuites(ciphers);

					// logger.debug("Reconnecting with, replicaId:{}", remoteId);

					this.socketSSL.addHandshakeCompletedListener(new HandshakeCompletedListener() {
						@Override
						public void handshakeCompleted(HandshakeCompletedEvent event) {
							logger.debug("SSL/TLS handshake complete (ServerConnectionSSLTLS)!, Id:{}"
									+ "  ## CipherSuite: {}, ", remoteId, event.getCipherSuite());
						}
					});

					this.socketSSL.startHandshake();

					ServersCommunicationLayerSSLTLS.setSSLSocketOptions(this.socketSSL);
					new DataOutputStream(this.socketSSL.getOutputStream())
							.writeInt(this.controller.getStaticConf().getProcessId());

					// ******* EDUARDO END **************//
				} else {
					socketSSL = newSocket;
				}
			} catch (UnknownHostException ex) {
				logger.error("Failed to connect to replica", ex);
			} catch (IOException ex) {

				logger.error("Impossible to reconnect to replica " + remoteId + ": " + ex.getMessage());
				// ex.printStackTrace();
			}

			if (socketSSL != null) {
				try {
					socketOutStream = new DataOutputStream(socketSSL.getOutputStream());
					socketInStream = new DataInputStream(socketSSL.getInputStream());

					// authKey = null;
					// authenticateAndEstablishAuthKey();
				} catch (IOException ex) {
					logger.error("Failed to authenticate to replica", ex);
				}
			}
		}

		connectLock.unlock();
	}

	private void closeSocket() {
		if (socketSSL != null) {
			try {
				socketOutStream.flush();
				socketSSL.close();
			} catch (IOException ex) {
				logger.debug("Error closing socket to " + remoteId);
			} catch (NullPointerException npe) {
				logger.debug("Socket already closed");
			}

			socketSSL = null;
			socketOutStream = null;
			socketInStream = null;
		}
	}

	private void waitAndConnect() {
		if (doWork) {
			try {
				Thread.sleep(POOL_TIME);
			} catch (InterruptedException ie) {
			}

			outQueue.clear();
			reconnect(null);
		}
	}

	/**
	 * Thread used to send packets to the remote server.
	 */
	private class SenderThread extends Thread {

		public SenderThread() {
			super("Sender for " + remoteId);
		}

		@Override
		public void run() {
			byte[] data = null;

			while (doWork) {
				// get a message to be sent
				try {
					data = outQueue.poll(POOL_TIME, TimeUnit.MILLISECONDS);
				} catch (InterruptedException ex) {
				}

				if (data != null) {
					// sendBytes(data, noMACs.contains(System.identityHashCode(data)));
					int ref = System.identityHashCode(data);
					boolean sendMAC = !noMACs.remove(ref);
					logger.trace((sendMAC ? "Sending" : "Not sending") + " MAC for data " + ref);
					logger.debug("Sending data to, RemoteId:{}", remoteId);
					sendBytes(data, sendMAC);
				}
			}

			logger.debug("Sender for " + remoteId + " stopped!");
		}
	}

	/**
	 * Thread used to receive packets from the remote server.
	 */
	protected class ReceiverThread extends Thread {

		public ReceiverThread() {
			super("Receiver for " + remoteId);
		}

		@Override
		public void run() {

			while (doWork) {
				if (socketSSL != null && socketInStream != null) {
					try {
						// read data length
						int dataLength = socketInStream.readInt();
						byte[] data = new byte[dataLength];

						// read data
						int read = 0;
						do {
							read += socketInStream.read(data, read, dataLength - read);
						} while (read < dataLength);

						byte hasMAC = socketInStream.readByte();

						logger.debug("Read: {}, HasMAC: {}", read, hasMAC);

						SystemMessage sm = (SystemMessage) (new ObjectInputStream(new ByteArrayInputStream(data))
								.readObject());

						// The MAC verification it is done for the SSL/TLS protocol.
						sm.authenticated = true;

						if (sm.getSender() == remoteId) {
							if (!inQueue.offer(sm)) {
								logger.warn("Inqueue full (message from " + remoteId + " discarded).");
							} else {
								logger.trace("Message: {} queued, remoteId: {}", sm.toString(), sm.getSender());
							}
						}
					} catch (ClassNotFoundException ex) {
						logger.info("Invalid message received. Ignoring!");
						// invalid message received, just ignore;
					} catch (IOException ex) {
						if (doWork) {
							logger.debug("Closing socket and reconnecting");
							closeSocket();
							waitAndConnect();
						}
					}
				} else {
					waitAndConnect();
				}
			}
		}
	}

	// ******* EDUARDO BEGIN: special thread for receiving messages indicating the
	// entrance into the system, coming from the TTP **************//
	// Simly pass the messages to the replica, indicating its entry into the system
	// TODO: Ask eduardo why a new thread is needed!!!
	// TODO2: Remove all duplicated code

	/**
	 * Thread used to receive packets from the remote server.
	 */
	protected class TTPReceiverThread extends Thread {

		private ServiceReplica replica;

		public TTPReceiverThread(ServiceReplica replica) {
			super("TTPReceiver for " + remoteId);
			this.replica = replica;
		}

		@Override
		public void run() {
			byte[] receivedMac = null;
			try {
				receivedMac = new byte[TOMUtil.getMacFactory().getMacLength()];
			} catch (NoSuchAlgorithmException ex) {
			}

			while (doWork) {
				if (socketSSL != null && socketInStream != null) {
					try {
						// read data length
						int dataLength = socketInStream.readInt();

						byte[] data = new byte[dataLength];

						// read data
						int read = 0;
						do {
							read += socketInStream.read(data, read, dataLength - read);
						} while (read < dataLength);

						SystemMessage sm = (SystemMessage) (new ObjectInputStream(new ByteArrayInputStream(data))
								.readObject());

						if (sm.getSender() == remoteId) {
							// System.out.println("Mensagem recebia de: "+remoteId);
							/*
							 * if (!inQueue.offer(sm)) { bftsmart.tom.util.Logger.
							 * println("(ReceiverThread.run) in queue full (message from " + remoteId +
							 * " discarded).");
							 * System.out.println("(ReceiverThread.run) in queue full (message from " +
							 * remoteId + " discarded)."); }
							 */
							this.replica.joinMsgReceived((VMMessage) sm);
						}

						/*
						 * //read mac boolean result = true;
						 * 
						 * byte hasMAC = socketInStream.readByte(); if
						 * (controller.getStaticConf().getUseMACs() && hasMAC == 1) {
						 * 
						 * read = 0; do { read += socketInStream.read(receivedMac, read, macSize -
						 * read); } while (read < macSize);
						 * 
						 * result = Arrays.equals(macReceive.doFinal(data), receivedMac); }
						 * 
						 * if (result) { SystemMessage sm = (SystemMessage) (new ObjectInputStream(new
						 * ByteArrayInputStream(data)).readObject());
						 * 
						 * if (sm.getSender() == remoteId) {
						 * //System.out.println("Mensagem recebia de: "+remoteId); //if
						 * (!inQueue.offer(sm)) { //bftsmart.tom.util.Logger.
						 * println("(ReceiverThread.run) in queue full (message from " + remoteId +
						 * " discarded).");
						 * //System.out.println("(ReceiverThread.run) in queue full (message from " +
						 * remoteId + " discarded)."); //} this.replica.joinMsgReceived((VMMessage) sm);
						 * } } else { //TODO: violation of authentication... we should do something
						 * logger.warn("Violation of authentication in message received from " +
						 * remoteId); }
						 */
					} catch (ClassNotFoundException ex) {
						logger.error("Failed to deserialize message", ex);
					} catch (IOException ex) {
						// ex.printStackTrace();
						if (doWork) {
							closeSocket();
							waitAndConnect();
						}
					}
				} else {
					waitAndConnect();
				}
			}
		}
	}
	// ******* EDUARDO END **************//
}
