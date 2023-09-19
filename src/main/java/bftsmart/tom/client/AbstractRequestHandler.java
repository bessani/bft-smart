package bftsmart.tom.client;

import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.util.ServiceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public abstract class AbstractRequestHandler {
	protected final Logger logger = LoggerFactory.getLogger("bftsmart.proxy");
	protected final int me;
	protected final int session;
	protected final int sequenceId;
	protected final int operationId;
	protected final int viewId;
	protected final TOMMessageType requestType;
	private final int timeout;
	protected final int[] replicas;
	private final Map<Integer, Integer> replicaIndex;
	protected final TOMMessage[] replies;
	protected final int replyQuorumSize;
	protected final Semaphore semaphore;
	protected Set<Integer> replySenders;
	protected ServiceResponse response;
	private boolean requestTimeout;
	protected boolean thereIsReplicaSpecificContent;

	public AbstractRequestHandler(int me, int session, int sequenceId, int operationId,
								  int viewId, TOMMessageType requestType, int timeout, int[] replicas,
								  int replyQuorumSize) {
		this.me = me;
		this.session = session;
		this.sequenceId = sequenceId;
		this.operationId = operationId;
		this.viewId = viewId;
		this.requestType = requestType;
		this.timeout = timeout;
		this.replicas = replicas;
		this.replies = new TOMMessage[replicas.length];
		this.replyQuorumSize = replyQuorumSize;
		this.semaphore = new Semaphore(0);
		this.replySenders = new HashSet<>(replicas.length);
		this.replicaIndex = new HashMap<>(replicas.length);
		for (int i = 0; i < replicas.length; i++) {
			replicaIndex.put(replicas[i], i);
		}
	}

	public abstract TOMMessage createRequest(byte[] request, boolean hasReplicaSpecificContent, byte metadata);

	public void waitForResponse() throws InterruptedException {
		if (!semaphore.tryAcquire(timeout, TimeUnit.SECONDS)) {
			requestTimeout = true;
		}
	}

	public void responseIsReady() {
		semaphore.release();
	}

	public ServiceResponse processReply(TOMMessage reply) {
		logger.debug("(current reqId: {}) Received reply from {} with reqId: {}", sequenceId, reply.getSender(),
				reply.getSequence());
		Integer i = replicaIndex.get(reply.getSender());
		if (i == null) {
			logger.error("Received reply from unknown replica {}", reply.getSender());
			return null;
		}
		if (sequenceId != reply.getSequence() || requestType != reply.getReqType()) {
			logger.debug("Ignoring reply from {} with reqId {}. Currently wait reqId {} of type {}",
					reply.getSender(), reply.getSequence(), sequenceId, requestType);
			return null;
		}
		if (replySenders.contains(reply.getSender())) {//process same reply only once
			return null;
		}
		replySenders.add(reply.getSender());
		replies[i] = reply;

		if (reply.getReplicaSpecificContent() != null) {
			thereIsReplicaSpecificContent = true;
		}

		return processReply(reply, i);
	}

	protected abstract ServiceResponse processReply(TOMMessage reply, int lastReceivedIndex);

	public int getSequenceId() {
		return sequenceId;
	}

	public int getNumberReceivedReplies() {
		return replySenders.size();
	}

	public int getReplyQuorumSize() {
		return replyQuorumSize;
	}

	/**
	 *
	 * Call this method after calling waitForResponse().
	 * @return true if request timeout or false otherwise.
	 * @requires Call this method after calling waitForResponse().
	 */
	public boolean isRequestTimeout() {
		return requestTimeout;
	}

	public abstract void printState();
}
