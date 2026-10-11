package io.github.wildfly8.brokerbridge;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The last line of defence against a connection that stops without a word. Every {@code every} it checks two things, each by
 * itself of the connection code: that the IB client answers a question that needs nothing but its monitor, and that the
 * connection loop still goes round. When either fails it calls {@code onStuck} once, and the caller ends the process: Docker's
 * restart policy starts a new bridge in seconds, where a stuck one costs every price and every order until someone looks
 * (2026-10-07: three days, with the Gateway logged in all the time).
 */
final class Watchdog {

	private static final Logger log = LoggerFactory.getLogger(Watchdog.class);

	private final IbClient client;
	private final IbConnection connection;
	private final Duration every;
	private final Duration probeTimeout;
	private final Duration loopStall;
	private final Consumer<String> onStuck;
	private volatile boolean running;
	private volatile Thread thread;

	/**
	 * @param every checks this often
	 * @param probeTimeout the client's monitor may be held this long (a handshake holds it for up to the handshake timeout)
	 * @param loopStall the connection loop may go this long without going round (one handshake, a wait and a backoff fit)
	 * @param onStuck gets the reason, once
	 */
	Watchdog(IbClient client, IbConnection connection, Duration every, Duration probeTimeout, Duration loopStall,
			Consumer<String> onStuck) {
		this.client = client;
		this.connection = connection;
		this.every = every;
		this.probeTimeout = probeTimeout;
		this.loopStall = loopStall;
		this.onStuck = onStuck;
	}

	void start() {
		running = true;
		Thread t = new Thread(this::run, "bridge-watchdog");
		t.setDaemon(true);
		thread = t;
		t.start();
	}

	void stop() {
		running = false;
		Thread t = thread;
		if (t != null) {
			t.interrupt();
		}
	}

	private void run() {
		while (running) {
			try {
				Thread.sleep(every);
			} catch (InterruptedException e) {
				return;
			}
			String problem = check();
			if (problem != null && running) {
				log.error("Watchdog: {}", problem);
				onStuck.accept(problem);
				return;
			}
		}
	}

	/** What is wrong, or null when the connection machinery answers. */
	String check() {
		CountDownLatch answered = new CountDownLatch(1);
		Thread probe = new Thread(() -> {
			client.isConnected();
			answered.countDown();
		}, "bridge-watchdog-probe");
		probe.setDaemon(true);
		probe.start();
		try {
			if (!answered.await(probeTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
				return "the IB client has not answered isConnected() for " + seconds(probeTimeout.toMillis())
						+ ": its monitor is held by a thread that waits for something else (a deadlock)";
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return null;
		}
		if (connection.running() && connection.loopAgeMillis() > loopStall.toMillis()) {
			return "the connection loop has not gone round for " + seconds(connection.loopAgeMillis());
		}
		return null;
	}

	private static String seconds(long millis) {
		return String.format(java.util.Locale.ROOT, "%.1f s", millis / 1000.0);
	}
}
