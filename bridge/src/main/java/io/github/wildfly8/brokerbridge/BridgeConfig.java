package io.github.wildfly8.brokerbridge;

import java.util.Map;

/** Bridge settings from environment variables. Invalid values fail startup. */
/**
 * {@code dataDir} null: order events are kept for replay in memory only (lost on restart). {@code watchdog}: the bridge ends
 * itself when its connection machinery is stuck (see {@link Watchdog}); only {@code BRIDGE_WATCHDOG=off} turns it off.
 */
public record BridgeConfig(String ibHost, int ibPort, int ibClientId, String bind, int port, boolean ordersEnabled,
		int marketDataType, String dataDir, int replayMax, boolean watchdog) {

	public static BridgeConfig fromEnv(Map<String, String> env) {
		return new BridgeConfig(
				text(env, "IB_HOST", "127.0.0.1"),
				port(env, "IB_PORT", 4002),
				integer(env, "IB_CLIENT_ID", 11, 0, Integer.MAX_VALUE),
				text(env, "BRIDGE_BIND", "127.0.0.1"),
				port(env, "BRIDGE_PORT", 8090),
				"true".equalsIgnoreCase(text(env, "BRIDGE_ORDERS_ENABLED", "false")),
				integer(env, "IB_MARKET_DATA_TYPE", 1, 1, 4),
				env.get("BRIDGE_DATA_DIR") == null || env.get("BRIDGE_DATA_DIR").isBlank() ? null : env.get("BRIDGE_DATA_DIR").trim(),
				integer(env, "BRIDGE_REPLAY_MAX", 100_000, 1, 10_000_000),
				!"off".equalsIgnoreCase(text(env, "BRIDGE_WATCHDOG", "on")));
	}

	private static String text(Map<String, String> env, String key, String def) {
		String v = env.get(key);
		return v == null || v.isBlank() ? def : v.trim();
	}

	private static int port(Map<String, String> env, String key, int def) {
		return integer(env, key, def, 1, 65535);
	}

	private static int integer(Map<String, String> env, String key, int def, int min, int max) {
		String v = env.get(key);
		if (v == null || v.isBlank()) {
			return def;
		}
		try {
			int n = Integer.parseInt(v.trim());
			if (n < min || n > max) {
				throw new IllegalArgumentException(key + " out of range [" + min + ", " + max + "]: " + v);
			}
			return n;
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(key + " is not a number: " + v);
		}
	}
}
