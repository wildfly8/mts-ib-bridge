package io.github.wildfly8.brokerbridge;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps one IB session up: connects, reconnects with backoff, resubscribes.
 * <p>
 * Locking: {@code wake} guards only the flag that {@link #closed()} sets, and nothing else is done while it is held. IB's
 * client takes its own monitor in {@code isConnected()}, and {@code eDisconnect()} calls {@link #closed()} while it holds
 * that monitor, so a thread that held {@code wake} and then asked the client would deadlock with the one that closes the
 * socket. The bridge on the VM stopped that way (2026-10-07: the Gateway closed the socket while the loop was between
 * connecting and waiting) and stayed disconnected for three days; see {@code IbConnectionLockTest}.
 */
final class IbConnection implements IbCallbacks.SessionListener {

	private static final Logger log = LoggerFactory.getLogger(IbConnection.class);

	/** How long a connected session waits before the loop looks at the socket again, if nothing wakes it. */
	static final long CHECK_MS = 30_000;

	private final BridgeConfig config;
	private final IbClient client;
	private final BridgeState state;
	private final EventHub hub;
	private final BrokerService broker;
	private final Duration minBackoff;
	private final Duration maxBackoff;
	/** Guards {@link #signalled}; no call into the client and no other lock while it is held. */
	private final Object wake = new Object();
	/** A {@link #closed()} or {@link #stop()} the loop has not looked at yet. */
	private boolean signalled;
	private volatile boolean running;
	private volatile Thread loop;
	/** Set when the Gateway closes the socket, including a 502 during the handshake. */
	private volatile boolean dropped;
	/** When the loop last went round ({@link System#nanoTime()}): what the watchdog reads. */
	private volatile long beat = System.nanoTime();

	IbConnection(BridgeConfig config, IbClient client, BridgeState state, EventHub hub, BrokerService broker,
			Duration minBackoff, Duration maxBackoff) {
		this.config = config;
		this.client = client;
		this.state = state;
		this.hub = hub;
		this.broker = broker;
		this.minBackoff = minBackoff;
		this.maxBackoff = maxBackoff;
	}

	void start() {
		beat = System.nanoTime();
		running = true;
		loop = Thread.ofVirtual().name("ib-connection").start(this::run);
	}

	void stop() {
		running = false;
		signal();
		if (loop != null) {
			loop.interrupt();
		}
		if (client.isConnected()) {
			client.disconnect();
		}
	}

	boolean running() {
		return running;
	}

	/** Milliseconds since the loop last went round. The longest legitimate stretch is one handshake, 30 s of waiting and one backoff. */
	long loopAgeMillis() {
		return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - beat);
	}

	private void run() {
		Duration backoff = minBackoff;
		while (running) {
			beat = System.nanoTime();
			try {
				if (!client.isConnected()) {
					if (connect()) {
						backoff = minBackoff;
					} else {
						backoff = pause(backoff);
						continue;
					}
				}
				// Wait for the session to drop. The client is asked here, outside the lock that closed() needs.
				if (running && client.isConnected()) {
					await(CHECK_MS);
				}
				// A handshake the Gateway accepts and then drops (error 502 during its daily restart) used to loop
				// straight back into connect(). That opened a new API client every few dozen milliseconds, filled the
				// Gateway's client table, and left the session down.
				if (running && !client.isConnected()) {
					backoff = pause(backoff);
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			} catch (Throwable t) {
				// A loop that ends is a bridge that never reconnects, and nothing says so: keep going.
				log.error("IB connection loop failed, trying again: {}", t.toString(), t);
				try {
					backoff = pause(backoff);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return;
				}
			}
		}
	}

	/** Wakes the loop. Takes {@code wake} for the flag alone. */
	private void signal() {
		synchronized (wake) {
			signalled = true;
			wake.notifyAll();
		}
	}

	/** Waits for {@link #signal()} or the time, whichever comes first; a signal that came before is not lost. */
	private void await(long millis) throws InterruptedException {
		synchronized (wake) {
			long left = TimeUnit.MILLISECONDS.toNanos(millis);
			long end = System.nanoTime() + left;
			while (!signalled && running && left > 0) {
				TimeUnit.NANOSECONDS.timedWait(wake, left);
				left = end - System.nanoTime();
			}
			signalled = false;
		}
	}

	/** Sleeps, then returns the next backoff (doubled, capped at {@link #maxBackoff}). */
	private Duration pause(Duration backoff) throws InterruptedException {
		log.info("Retrying IB connection in {} s", backoff.toSeconds());
		beat = System.nanoTime();
		Thread.sleep(backoff);
		Duration next = backoff.multipliedBy(2);
		return next.compareTo(maxBackoff) > 0 ? maxBackoff : next;
	}

	private boolean connect() {
		dropped = false;
		log.info("Connecting to IB at {}:{} as client {}", config.ibHost(), config.ibPort(), config.ibClientId());
		try {
			if (!client.connect(config.ibHost(), config.ibPort(), config.ibClientId())) {
				return false;
			}
		} catch (RuntimeException e) {
			log.warn("IB connect failed: {}", e.toString());
			return false;
		}
		if (dropped || !client.isConnected()) {
			return dropHandshake();
		}
		client.reqMarketDataType(config.marketDataType());
		client.reqIds();
		// fills made while the bridge was disconnected; already-delivered ones are dropped by fill id
		client.reqExecutions(state.newRequestId(), config.ibClientId());
		if (dropped || !client.isConnected()) {
			return dropHandshake();
		}
		// Restore streams before the session is marked up. A reader that sees connected
		// otherwise races the resubscribe that follows the flag.
		broker.resubscribe();
		if (dropped || !client.isConnected()) {
			return dropHandshake();
		}
		state.connected(true, client.serverVersion());
		state.lastError(null);
		log.info("Connected to IB, server version {}", client.serverVersion());
		hub.publish("connection", broker.status());
		return true;
	}

	/** The Gateway closed the socket during the handshake. Release the client id and do not mark the session up. */
	private boolean dropHandshake() {
		log.warn("IB dropped the handshake; waiting before another attempt");
		try {
			client.disconnect();
		} catch (RuntimeException e) {
			log.warn("IB disconnect after a dropped handshake: {}", e.toString());
		}
		state.connected(false, null);
		return false;
	}

	/** Test seam: whether the calling thread holds the lock of the connection loop. */
	boolean callerHoldsLock() {
		return Thread.holdsLock(wake);
	}

	@Override
	public void closed() {
		dropped = true;
		signal();
	}

	@Override
	public void restored(boolean dataLost) {
		if (dataLost) {
			Thread.ofVirtual().name("ib-resubscribe").start(broker::resubscribe);
		}
	}
}
