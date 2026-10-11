package io.github.wildfly8.brokerbridge;

import java.net.InetAddress;
import java.nio.file.Path;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * broker-bridge: a separate GPLv3 program that talks to IB Gateway/TWS with IB's official client and offers a
 * broker-neutral HTTP + SSE API on localhost.
 */
public final class BridgeMain {

	static {
		// must run before the first logger is created (slf4j-simple reads system properties once)
		String level = System.getenv("LOG_LEVEL");
		if (level != null && !level.isBlank() && System.getProperty("org.slf4j.simpleLogger.defaultLogLevel") == null) {
			System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", level.trim().toLowerCase());
		}
	}

	private static final Logger log = LoggerFactory.getLogger(BridgeMain.class);

	private BridgeMain() {}

	public static void main(String[] args) throws Exception {
		BridgeConfig config;
		try {
			config = BridgeConfig.fromEnv(System.getenv());
		} catch (IllegalArgumentException e) {
			System.err.println("Invalid configuration: " + e.getMessage());
			System.exit(2);
			return;
		}
		if (!InetAddress.getByName(config.bind()).isLoopbackAddress()) {
			log.warn("BRIDGE_BIND={} is not a loopback address: the bridge API has no authentication; "
					+ "make sure nothing outside this host can reach it", config.bind());
		}
		BridgeState state = new BridgeState();
		EventLog eventLog;
		if (config.dataDir() == null) {
			log.warn("BRIDGE_DATA_DIR is not set: order events can be replayed after a client reconnect, "
					+ "but not after a bridge restart");
			eventLog = EventLog.inMemory(config.replayMax(), System.currentTimeMillis());
		} else {
			eventLog = EventLog.open(Path.of(config.dataDir()), config.replayMax(), System.currentTimeMillis());
		}
		EventHub hub = new EventHub(eventLog);
		IbCallbacks callbacks = new IbCallbacks(state, hub, config.ordersEnabled());
		IbClient client = new SocketIbClient(callbacks);
		BrokerService broker = new BrokerService(client, state, config.ordersEnabled(), eventLog);
		IbConnection connection = new IbConnection(config, client, state, hub, broker, Duration.ofSeconds(5),
				Duration.ofSeconds(120));
		callbacks.listener(connection);
		BridgeHttpServer server = new BridgeHttpServer(config.bind(), config.port(), broker, hub, 15_000);

		log.info("broker-bridge starting: IB {}:{} client {}, orders {}, market data type {}", config.ibHost(),
				config.ibPort(), config.ibClientId(), config.ordersEnabled() ? "ENABLED" : "disabled",
				config.marketDataType());
		server.start();
		connection.start();
		if (config.watchdog()) {
			// Runtime.halt, not System.exit: the shutdown hook below disconnects the IB client, which needs the monitor that is stuck
			new Watchdog(client, connection, Duration.ofSeconds(15), Duration.ofSeconds(45), Duration.ofMinutes(5), reason -> {
				log.error("The bridge ends itself so that its container restarts it. Threads:\n{}", ThreadDump.text());
				System.err.flush();
				Runtime.getRuntime().halt(70);
			}).start();
		} else {
			log.warn("BRIDGE_WATCHDOG=off: a stuck connection will not restart the bridge");
		}
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			log.info("broker-bridge stopping");
			connection.stop();
			server.stop();
			eventLog.close();
		}, "shutdown"));
		Thread.currentThread().join();
	}
}
