package io.github.wildfly8.brokerbridge;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.nio.file.Files;
import java.nio.file.Path;

import com.sun.management.HotSpotDiagnosticMXBean;

/** The text of a thread dump, for the log of a process that is about to end because it is stuck. */
final class ThreadDump {

	private ThreadDump() {}

	/** All threads with their locks, virtual threads included when the JVM can say so. */
	static String text() {
		try {
			Path file = Path.of(System.getProperty("java.io.tmpdir"), "bridge-threads-" + System.nanoTime() + ".txt");
			try {
				ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class).dumpThreads(file.toString(),
						HotSpotDiagnosticMXBean.ThreadDumpFormat.TEXT_PLAIN);
				return Files.readString(file);
			} finally {
				Files.deleteIfExists(file);
			}
		} catch (IOException | RuntimeException | LinkageError e) {
			return platformThreads() + "\n(the JVM's own thread dump failed: " + e + ")";
		}
	}

	/** Platform threads only, but with who holds which monitor and what each waits for. */
	static String platformThreads() {
		StringBuilder out = new StringBuilder();
		for (ThreadInfo t : ManagementFactory.getThreadMXBean().dumpAllThreads(true, true)) {
			out.append('"').append(t.getThreadName()).append("\" ").append(t.getThreadState());
			if (t.getLockName() != null) {
				out.append(" on ").append(t.getLockName());
			}
			if (t.getLockOwnerName() != null) {
				out.append(" held by \"").append(t.getLockOwnerName()).append('"');
			}
			out.append('\n');
			for (StackTraceElement frame : t.getStackTrace()) {
				out.append("\tat ").append(frame).append('\n');
			}
			for (var monitor : t.getLockedMonitors()) {
				out.append("\t- locked ").append(monitor).append('\n');
			}
			out.append('\n');
		}
		return out.toString();
	}
}
