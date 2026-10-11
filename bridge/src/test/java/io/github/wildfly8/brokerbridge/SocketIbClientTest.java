package io.github.wildfly8.brokerbridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.Test;

import io.github.wildfly8.brokerbridge.Model.Instrument;

/** The real IB client against a minimal fake Gateway speaking the v100+ handshake (text messages). */
class SocketIbClientTest {

	private static final int SERVER_VERSION = 176;

	/** Accepts one client, completes the handshake, sends next id + accounts, records request message ids. */
	static final class FakeGateway implements AutoCloseable {
		final ServerSocket server;
		final List<String> received = new CopyOnWriteArrayList<>();
		volatile String header;
		private volatile Socket socket;

		FakeGateway() throws IOException {
			server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
			Thread.ofVirtual().start(this::serve);
		}

		int port() {
			return server.getLocalPort();
		}

		private void serve() {
			try (Socket s = server.accept()) {
				socket = s;
				DataInputStream in = new DataInputStream(s.getInputStream());
				DataOutputStream out = new DataOutputStream(s.getOutputStream());
				byte[] api = in.readNBytes(4);
				header = new String(api, StandardCharsets.US_ASCII) + new String(frame(in), StandardCharsets.US_ASCII);
				send(out, SERVER_VERSION + "", "20261003 16:00:00 EST");
				received.add(firstField(frame(in))); // START_API
				send(out, "9", "1", "5");
				send(out, "15", "1", "DU1234567");
				while (true) {
					received.add(firstField(frame(in)));
				}
			} catch (IOException e) {
				// client or test closed the socket
			}
		}

		void dropClient() throws IOException {
			socket.close();
		}

		private static byte[] frame(DataInputStream in) throws IOException {
			return in.readNBytes(in.readInt());
		}

		private static String firstField(byte[] msg) {
			String s = new String(msg, StandardCharsets.US_ASCII);
			return s.substring(0, s.indexOf('\0'));
		}

		private static void send(DataOutputStream out, String... fields) throws IOException {
			ByteArrayOutputStream b = new ByteArrayOutputStream();
			for (String f : fields) {
				b.writeBytes(f.getBytes(StandardCharsets.US_ASCII));
				b.write(0);
			}
			out.writeInt(b.size());
			out.write(b.toByteArray());
			out.flush();
		}

		@Override
		public void close() throws IOException {
			server.close();
		}
	}

	private static void waitFor(BooleanSupplier cond) throws InterruptedException {
		long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (!cond.getAsBoolean() && System.nanoTime() < until) {
			Thread.sleep(10);
		}
		assertTrue(cond.getAsBoolean());
	}

	@Test
	void handshakeCallbacksRequestsAndClose() throws Exception {
		BridgeState state = new BridgeState();
		IbCallbacks callbacks = new IbCallbacks(state, new EventHub(EventLog.inMemory(1000, System.currentTimeMillis())), false);
		SocketIbClient client = new SocketIbClient(callbacks);
		try (FakeGateway gw = new FakeGateway()) {
			assertTrue(client.connect("127.0.0.1", gw.port(), 11));
			assertTrue(gw.header.startsWith("API\0v100.."), gw.header);
			assertEquals(SERVER_VERSION, client.serverVersion());
			state.connected(true, client.serverVersion());

			waitFor(() -> state.nextOrderId.get() == 5);
			waitFor(() -> "DU*****67".equals(state.status(false).accounts()));
			assertEquals("71", gw.received.get(0), "START_API");

			client.reqMktData(1001, Contracts.toIb(Instrument.stock("SPY", "SMART", "USD")), "", true);
			waitFor(() -> gw.received.contains("1"));  // REQ_MKT_DATA

			gw.dropClient();
			// A dropped socket can arrive as error 502 before EClient reports the close. 502 marks the
			// session down immediately; the socket flag follows. Wait for both, or the assertion races.
			waitFor(() -> !state.connected() && !client.isConnected());
		}
	}

	/** The Gateway of its restart: accepts the socket and says nothing. The handshake must end, and release the monitor. */
	@Test
	void aGatewayThatAcceptsAndSaysNothingIsGivenUpOn() throws Exception {
		BridgeState state = new BridgeState();
		IbCallbacks callbacks = new IbCallbacks(state, new EventHub(EventLog.inMemory(1000, System.currentTimeMillis())), false);
		SocketIbClient client = new SocketIbClient(callbacks, 2_000, 300);
		AtomicBoolean closedByClient = new AtomicBoolean();
		try (ServerSocket silent = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			Thread.ofVirtual().start(() -> {
				try (Socket s = silent.accept()) {
					closedByClient.set(s.getInputStream().read() < 0); // the client sent its handshake bytes; 'closed' is EOF after them
					while (s.getInputStream().read() >= 0) {
						// drain what the client sent before it hung up
					}
					closedByClient.set(true);
				} catch (IOException e) {
					closedByClient.set(true);
				}
			});
			long started = System.nanoTime();
			assertFalse(client.connect("127.0.0.1", silent.getLocalPort(), 11));
			long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
			assertTrue(ms >= 250 && ms < 5_000, "the handshake is given up after its timeout, not before and not never: " + ms + " ms");
			assertFalse(client.isConnected());
			waitFor(closedByClient::get);
		}
	}

	@Test
	void aRefusedConnectionIsReportedAtOnce() throws Exception {
		BridgeState state = new BridgeState();
		IbCallbacks callbacks = new IbCallbacks(state, new EventHub(EventLog.inMemory(1000, System.currentTimeMillis())), false);
		SocketIbClient client = new SocketIbClient(callbacks, 2_000, 300);
		int port;
		try (ServerSocket s = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			port = s.getLocalPort();
		}
		assertFalse(client.connect("127.0.0.1", port, 11));
		assertFalse(client.isConnected());
	}

	/** The timeout is for the handshake: a session that is quiet for longer than it must stay up. */
	@Test
	void aQuietSessionIsNotTimedOutAfterTheHandshake() throws Exception {
		BridgeState state = new BridgeState();
		IbCallbacks callbacks = new IbCallbacks(state, new EventHub(EventLog.inMemory(1000, System.currentTimeMillis())), false);
		SocketIbClient client = new SocketIbClient(callbacks, 2_000, 300);
		try (FakeGateway gw = new FakeGateway()) {
			assertTrue(client.connect("127.0.0.1", gw.port(), 11));
			Thread.sleep(900);
			assertTrue(client.isConnected(), "three handshake timeouts of silence later");
			client.reqMktData(1001, Contracts.toIb(Instrument.stock("SPY", "SMART", "USD")), "", true);
			waitFor(() -> gw.received.contains("1"));
			client.disconnect();
		}
	}
}
