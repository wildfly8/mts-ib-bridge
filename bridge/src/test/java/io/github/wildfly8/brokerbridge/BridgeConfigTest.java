package io.github.wildfly8.brokerbridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

class BridgeConfigTest {

	@Test
	void defaultsAreLocalPaperAndOrdersOff() {
		BridgeConfig c = BridgeConfig.fromEnv(Map.of());
		assertEquals("127.0.0.1", c.ibHost());
		assertEquals(4002, c.ibPort());
		assertEquals(11, c.ibClientId());
		assertEquals("127.0.0.1", c.bind());
		assertEquals(8090, c.port());
		assertFalse(c.ordersEnabled());
		assertEquals(1, c.marketDataType());
		assertEquals(null, c.dataDir());
		assertEquals(100_000, c.replayMax());
		assertTrue(c.watchdog(), "the watchdog is on unless BRIDGE_WATCHDOG=off");
	}

	@Test
	void onlyOffTurnsTheWatchdogOff() {
		assertFalse(BridgeConfig.fromEnv(Map.of("BRIDGE_WATCHDOG", "off")).watchdog());
		assertFalse(BridgeConfig.fromEnv(Map.of("BRIDGE_WATCHDOG", " OFF ")).watchdog());
		assertTrue(BridgeConfig.fromEnv(Map.of("BRIDGE_WATCHDOG", "on")).watchdog());
		assertTrue(BridgeConfig.fromEnv(Map.of("BRIDGE_WATCHDOG", "false")).watchdog(), "a typo does not switch the safety net off");
	}

	@Test
	void durableReplaySettings() {
		BridgeConfig c = BridgeConfig.fromEnv(Map.of("BRIDGE_DATA_DIR", " /data ", "BRIDGE_REPLAY_MAX", "500"));
		assertEquals("/data", c.dataDir());
		assertEquals(500, c.replayMax());
		assertThrows(IllegalArgumentException.class, () -> BridgeConfig.fromEnv(Map.of("BRIDGE_REPLAY_MAX", "0")));
	}

	@Test
	void onlyTrueEnablesOrders() {
		assertTrue(BridgeConfig.fromEnv(Map.of("BRIDGE_ORDERS_ENABLED", "true")).ordersEnabled());
		assertTrue(BridgeConfig.fromEnv(Map.of("BRIDGE_ORDERS_ENABLED", "TRUE")).ordersEnabled());
		assertFalse(BridgeConfig.fromEnv(Map.of("BRIDGE_ORDERS_ENABLED", "yes")).ordersEnabled());
		assertFalse(BridgeConfig.fromEnv(Map.of("BRIDGE_ORDERS_ENABLED", "1")).ordersEnabled());
	}

	@Test
	void readsOverrides() {
		BridgeConfig c = BridgeConfig.fromEnv(Map.of("IB_HOST", "gw", "IB_PORT", "4001", "IB_CLIENT_ID", "21",
				"BRIDGE_BIND", "0.0.0.0", "BRIDGE_PORT", "9000", "IB_MARKET_DATA_TYPE", "3"));
		assertEquals("gw", c.ibHost());
		assertEquals(4001, c.ibPort());
		assertEquals(21, c.ibClientId());
		assertEquals("0.0.0.0", c.bind());
		assertEquals(9000, c.port());
		assertEquals(3, c.marketDataType());
	}

	@Test
	void rejectsInvalidValues() {
		assertThrows(IllegalArgumentException.class, () -> BridgeConfig.fromEnv(Map.of("IB_PORT", "abc")));
		assertThrows(IllegalArgumentException.class, () -> BridgeConfig.fromEnv(Map.of("BRIDGE_PORT", "70000")));
		assertThrows(IllegalArgumentException.class, () -> BridgeConfig.fromEnv(Map.of("IB_MARKET_DATA_TYPE", "5")));
		assertThrows(IllegalArgumentException.class, () -> BridgeConfig.fromEnv(Map.of("IB_CLIENT_ID", "-1")));
	}
}
