package io.github.wildfly8.brokerbridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.Test;

/**
 * The lock order between the connection loop and IB's client. EClientSocket.isConnected() and eDisconnect() are synchronized on
 * the client, and eDisconnect() calls connectionClosed() (so IbConnection.closed()) while it holds that monitor. A loop that asks
 * the client while it holds the lock closed() needs is a deadlock waiting for the Gateway to close the socket at that moment:
 * the bridge on the VM stopped that way on 2026-10-07 and stayed "not connected to the broker" for three days.
 */
class IbConnectionLockTest {

	/** Locks the way EClientSocket does, and records every call made while the connection loop's lock is held. */
	static final class MonitorClient extends FakeIbClient {
		volatile IbConnection connection;
		final AtomicInteger callsUnderLoopLock = new AtomicInteger();

		private void note() {
			IbConnection c = connection;
			if (c != null && c.callerHoldsLock()) {
				callsUnderLoopLock.incrementAndGet();
			}
		}

		@Override
		public boolean connect(String host, int port, int clientId) {
			note();
			connectAttempts++;
			synchronized (this) {
				connected = true;
			}
			return true;
		}

		@Override
		public boolean isConnected() {
			note();
			synchronized (this) {
				return connected;
			}
		}

		@Override
		public void disconnect() {
			note();
			synchronized (this) {
				connected = false;
			}
		}

		/** What EReader does when the Gateway closes the socket: eDisconnect() holds the monitor while it tells the listener. */
		void gatewayCloses() {
			synchronized (this) {
				connected = false;
				connection.closed();
			}
		}
	}

	private static void waitFor(BooleanSupplier cond, long seconds) throws InterruptedException {
		long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
		while (!cond.getAsBoolean() && System.nanoTime() < until) {
			Thread.sleep(5);
		}
		assertTrue(cond.getAsBoolean());
	}

	private static IbConnection connection(MonitorClient ib, BridgeState state, Duration backoff) throws Exception {
		BridgeConfig config = BridgeConfig.fromEnv(java.util.Map.of());
		EventHub hub = new EventHub(EventLog.inMemory(1000, System.currentTimeMillis()));
		IbCallbacks callbacks = new IbCallbacks(state, hub, false);
		BrokerService broker = new BrokerService(ib, state, false, hub.eventLog());
		IbConnection conn = new IbConnection(config, ib, state, hub, broker, backoff, backoff);
		callbacks.listener(conn);
		ib.connection = conn;
		return conn;
	}

	@Test
	void theLoopNeverAsksTheClientWhileItHoldsTheLockThatClosedNeeds() throws Exception {
		assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
			MonitorClient ib = new MonitorClient();
			BridgeState state = new BridgeState();
			IbConnection conn = connection(ib, state, Duration.ofMillis(20));
			conn.start();
			try {
				waitFor(state::connected, 5);
				ib.gatewayCloses();
				waitFor(() -> ib.connectAttempts >= 2 && state.connected(), 5);
				ib.gatewayCloses();
				waitFor(() -> ib.connectAttempts >= 3 && state.connected(), 5);
			} finally {
				conn.stop();
			}
			assertEquals(0, ib.callsUnderLoopLock.get(), "calls into the IB client made while holding the connection loop's lock");
		});
	}

	@Test
	void aGatewayThatKeepsClosingTheSocketNeverStopsTheLoop() throws Exception {
		// on the unfixed loop this deadlocks within a few hundred closes, and stop() then deadlocks too: hence the timeout
		assertTimeoutPreemptively(Duration.ofSeconds(90), () -> {
			MonitorClient ib = new MonitorClient();
			BridgeState state = new BridgeState();
			IbConnection conn = connection(ib, state, Duration.ofNanos(1));
			AtomicBoolean stop = new AtomicBoolean();
			AtomicInteger closes = new AtomicInteger();
			// the Gateway of the nightly restart: accepts, then closes within a few hundred microseconds, again and again
			Thread gateway = new Thread(() -> {
				while (!stop.get()) {
					if (ib.connected) {
						LockSupport.parkNanos(ThreadLocalRandom.current().nextLong(0, 300_000));
						ib.gatewayCloses();
						closes.incrementAndGet();
					} else {
						Thread.onSpinWait();
					}
				}
			}, "fake-gateway");
			gateway.setDaemon(true);
			conn.start();
			gateway.start();
			try {
				waitFor(() -> closes.get() >= 1500, 60);
				stop.set(true);
				gateway.join(5_000);
				assertFalse(gateway.isAlive());
				// the Gateway stays up now: the loop must still be alive and get the session back
				waitFor(() -> ib.connected && state.connected(), 5);
			} finally {
				stop.set(true);
				conn.stop();
			}
		});
	}
}
