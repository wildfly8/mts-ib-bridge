package io.github.wildfly8.brokerbridge;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ib.client.Contract;
import com.ib.client.EClientSocket;
import com.ib.client.EJavaSignal;
import com.ib.client.EReader;
import com.ib.client.EWrapper;
import com.ib.client.ExecutionFilter;
import com.ib.client.Order;
import com.ib.client.OrderCancel;

/**
 * {@link IbClient} on IB's official client; decoded messages are dispatched on a virtual thread.
 * <p>
 * IB's {@code eConnect} reads the Gateway's first message with the client's monitor held, and {@code isConnected()} needs the
 * same monitor. A Gateway that accepts the socket and then says nothing (it does, while it restarts) would hold that monitor
 * for good, and every call that asks the client with it: the handshake is therefore made on a socket with timeouts.
 */
final class SocketIbClient implements IbClient {

	private static final Logger log = LoggerFactory.getLogger(SocketIbClient.class);

	/** How long the TCP connection may take. */
	static final int CONNECT_TIMEOUT_MS = 10_000;
	/** How long the Gateway may take to answer the handshake. */
	static final int HANDSHAKE_TIMEOUT_MS = 15_000;

	private final EJavaSignal signal = new EJavaSignal();
	private final EClientSocket socket;
	private final int connectTimeoutMs;
	private final int handshakeTimeoutMs;

	SocketIbClient(EWrapper wrapper) {
		this(wrapper, CONNECT_TIMEOUT_MS, HANDSHAKE_TIMEOUT_MS);
	}

	SocketIbClient(EWrapper wrapper, int connectTimeoutMs, int handshakeTimeoutMs) {
		this.socket = new EClientSocket(wrapper, signal);
		this.connectTimeoutMs = connectTimeoutMs;
		this.handshakeTimeoutMs = handshakeTimeoutMs;
	}

	@Override
	public boolean connect(String host, int port, int clientId) {
		Socket tcp = new Socket();
		try {
			tcp.connect(new InetSocketAddress(host, port), connectTimeoutMs);
			tcp.setSoTimeout(handshakeTimeoutMs);
			socket.eConnect(tcp, clientId);
			// a session may be silent for as long as the Gateway has nothing to say
			tcp.setSoTimeout(0);
		} catch (IOException | RuntimeException e) {
			log.warn("IB handshake failed: {}", e.toString());
			giveUp(tcp);
			return false;
		}
		if (!socket.isConnected()) {
			log.warn("IB handshake did not complete within {} ms", handshakeTimeoutMs);
			giveUp(tcp);
			return false;
		}
		EReader reader = new EReader(socket, signal);
		reader.start();
		Thread.ofVirtual().name("ib-dispatch").start(() -> {
			while (socket.isConnected()) {
				signal.waitForSignal();
				try {
					reader.processMsgs();
				} catch (Exception e) {
					log.warn("IB message processing failed: {}", Json.maskAccounts(String.valueOf(e)));
				}
			}
			log.info("IB dispatch loop ended");
		});
		return true;
	}

	/** Closes a socket whose handshake failed, and resets the IB client so that the next attempt starts clean. */
	private void giveUp(Socket tcp) {
		try {
			tcp.close();
		} catch (IOException ignored) {
			// closing a socket that is already gone
		}
		socket.eDisconnect();
	}

	@Override
	public void disconnect() {
		socket.eDisconnect();
		signal.issueSignal();
	}

	@Override
	public boolean isConnected() {
		return socket.isConnected();
	}

	@Override
	public int serverVersion() {
		return socket.serverVersion();
	}

	@Override
	public void reqMarketDataType(int type) {
		socket.reqMarketDataType(type);
	}

	@Override
	public void reqMktData(int reqId, Contract contract, String genericTicks, boolean snapshot) {
		socket.reqMktData(reqId, contract, genericTicks, snapshot, false, List.of());
	}

	@Override
	public void cancelMktData(int reqId) {
		socket.cancelMktData(reqId);
	}

	@Override
	public void reqHistoricalData(int reqId, Contract contract, String end, String duration, String barSize,
			String what, boolean regularHoursOnly) {
		socket.reqHistoricalData(reqId, contract, end, duration, barSize, what, regularHoursOnly ? 1 : 0, 1, false,
				List.of());
	}

	@Override
	public void cancelHistoricalData(int reqId) {
		socket.cancelHistoricalData(reqId);
	}

	@Override
	public void reqContractDetails(int reqId, Contract contract) {
		socket.reqContractDetails(reqId, contract);
	}

	@Override
	public void placeOrder(int ibOrderId, Contract contract, Order order) {
		socket.placeOrder(ibOrderId, contract, order);
	}

	@Override
	public void cancelOrder(int ibOrderId) {
		socket.cancelOrder(ibOrderId, new OrderCancel());
	}

	@Override
	public void reqIds() {
		socket.reqIds(-1);
	}

	@Override
	public void reqExecutions(int reqId, int clientId) {
		ExecutionFilter filter = new ExecutionFilter();
		filter.clientId(clientId);
		socket.reqExecutions(reqId, filter);
	}
}
