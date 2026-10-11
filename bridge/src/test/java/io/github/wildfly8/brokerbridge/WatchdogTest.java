package io.github.wildfly8.brokerbridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.Test;

class WatchdogTest {

	/** A client that does not answer: the monitor of IB's client held by a thread that waits for something else. */
	static final class HeldClient extends FakeIbClient {
		final CountDownLatch release = new CountDownLatch(1);

		@Override
		public boolean isConnected() {
			try {
				release.await();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			return false;
		}
	}

	/** A client whose connect never returns: the loop stops going round. */
	static final class StuckConnectClient extends FakeIbClient {
		final CountDownLatch release = new CountDownLatch(1);

		@Override
		public boolean connect(String host, int port, int clientId) {
			try {
				release.await();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			return false;
		}
	}

	private static IbConnection connection(FakeIbClient ib, Duration backoff) {
		BridgeConfig config = BridgeConfig.fromEnv(java.util.Map.of());
		BridgeState state = new BridgeState();
		EventHub hub = new EventHub(EventLog.inMemory(100, System.currentTimeMillis()));
		IbCallbacks callbacks = new IbCallbacks(state, hub, false);
		BrokerService broker = new BrokerService(ib, state, false, hub.eventLog());
		IbConnection conn = new IbConnection(config, ib, state, hub, broker, backoff, backoff);
		callbacks.listener(conn);
		return conn;
	}

	private static void waitFor(BooleanSupplier cond) throws InterruptedException {
		long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (!cond.getAsBoolean() && System.nanoTime() < until) {
			Thread.sleep(10);
		}
		assertTrue(cond.getAsBoolean());
	}

	@Test
	void aClientThatDoesNotAnswerIsReportedOnce() throws Exception {
		HeldClient ib = new HeldClient();
		IbConnection conn = connection(ib, Duration.ofMillis(20));
		List<String> reported = new CopyOnWriteArrayList<>();
		Watchdog dog = new Watchdog(ib, conn, Duration.ofMillis(10), Duration.ofMillis(150), Duration.ofMinutes(5), reported::add);
		dog.start();
		try {
			waitFor(() -> !reported.isEmpty());
			Thread.sleep(300);
			assertEquals(1, reported.size(), "reported once, then the watchdog is done: " + reported);
			assertTrue(reported.get(0).contains("isConnected()") && reported.get(0).contains("deadlock"), reported.get(0));
		} finally {
			dog.stop();
			ib.release.countDown();
		}
	}

	@Test
	void aLoopThatStoppedGoingRoundIsReported() throws Exception {
		StuckConnectClient ib = new StuckConnectClient();
		IbConnection conn = connection(ib, Duration.ofMillis(20));
		List<String> reported = new CopyOnWriteArrayList<>();
		Watchdog dog = new Watchdog(ib, conn, Duration.ofMillis(10), Duration.ofSeconds(5), Duration.ofMillis(200), reported::add);
		conn.start();
		dog.start();
		try {
			waitFor(() -> !reported.isEmpty());
			assertTrue(reported.get(0).contains("connection loop has not gone round"), reported.get(0));
		} finally {
			dog.stop();
			ib.release.countDown();
			conn.stop();
		}
	}

	@Test
	void aHealthyConnectionIsLeftAlone() throws Exception {
		FakeIbClient ib = new FakeIbClient();
		IbConnection conn = connection(ib, Duration.ofMillis(20));
		List<String> reported = new CopyOnWriteArrayList<>();
		Watchdog dog = new Watchdog(ib, conn, Duration.ofMillis(10), Duration.ofSeconds(5), Duration.ofSeconds(60), reported::add);
		conn.start();
		dog.start();
		try {
			waitFor(() -> ib.connected);
			Thread.sleep(500);
			assertTrue(reported.isEmpty(), "nothing is wrong: " + reported);
			assertNull(dog.check());
		} finally {
			dog.stop();
			conn.stop();
		}
	}

	@Test
	void aStoppedConnectionIsNotAStall() {
		FakeIbClient ib = new FakeIbClient();
		IbConnection conn = connection(ib, Duration.ofMillis(20));
		Watchdog dog = new Watchdog(ib, conn, Duration.ofSeconds(1), Duration.ofSeconds(5), Duration.ofMillis(1), reason -> { });
		assertNull(dog.check(), "a bridge whose loop was never started (or was stopped on purpose) is not stuck");
	}

	@Test
	void theThreadDumpNamesTheThreadsAndTheirLocks() {
		String dump = ThreadDump.text();
		assertNotNull(dump);
		assertTrue(dump.contains("main") || dump.contains("Test worker") || dump.contains("surefire"), dump.substring(0, Math.min(400, dump.length())));
		String platform = ThreadDump.platformThreads();
		assertTrue(platform.contains("\tat "), "stack frames are in it");
	}
}
