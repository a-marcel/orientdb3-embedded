package example;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ServerRandomPort {
	protected static Logger logger = LogManager.getLogger(ServerRandomPort.class);

	public static String[] setRandomPortInArguments(String[] args) {
		List<String> newArgs = new ArrayList<String>();

		Integer length = 0;
		if (null != args)
			length = args.length;

		Integer lastPos = 0;

		Boolean foundViaCommandLine = false;
		Integer port = null;

		if (null != args) {
			for (int i = 0; i < args.length; i++) {
				newArgs.add(args[i]);
				if (args[i].substring(0, 13).equals("--server.port")) {
					foundViaCommandLine = true;

					port = Integer.parseInt(args[i].substring(14, args[i].length()));
				}
				lastPos++;
			}
		}

		return newArgs.toArray(new String[newArgs.size()]);
	}

	public static Integer getFreePort() throws IOException {
		ServerSocket socket = new ServerSocket(0);

		socket.setReuseAddress(true);
		Integer port = socket.getLocalPort();
		socket.close();

		logger.debug(String.format("Found free port: %s", port.toString()));

		return port;
	}

	public static Integer getFreePortUnchecked() {
		try {
			return getFreePort();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}