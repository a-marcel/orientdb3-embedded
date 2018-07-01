package example;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.UUID;
import java.util.logging.Level;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerEntryConfiguration;
import com.orientechnologies.orient.server.config.OServerNetworkConfiguration;
import com.orientechnologies.orient.server.config.OServerNetworkListenerConfiguration;
import com.orientechnologies.orient.server.config.OServerNetworkProtocolConfiguration;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;

public class RemoteStoringTest {
	protected static final Logger logger = LogManager.getLogger(RemoteStoringTest.class);
	static int httpPort;
	static int binaryPort;

	@ClassRule
	public static TemporaryFolder folder = new TemporaryFolder();

	static OServer server;
	OrientGraph graph;

	private static String USERNAME = "root";
	private static String PASSWORD = "123";

	private static UUID ROOT_KEY = UUID.randomUUID();
	private static UUID LINK_KEY = UUID.randomUUID();

	@BeforeClass
	public static void initSystemViaConfig() throws Exception {

		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();

		java.util.logging.Logger orientDbLogger = java.util.logging.Logger.getLogger("com.orientechnologies");
		orientDbLogger.setLevel(Level.WARNING);

		httpPort = ServerRandomPort.getFreePort();
		binaryPort = ServerRandomPort.getFreePort();

		File tmpDir = folder.newFolder("temp");

		URL location = RemoteStoringTest.class.getProtectionDomain().getCodeSource().getLocation();
		String path = location.getFile().replace("test-classes/", "") + "studio/www";

		logger.info("Using studio www dir {}", path);
		logger.info("Use studio url http://localhost:{}/studio/index.html", httpPort);

		System.setProperty("orientdb.www.path", path);

		System.setProperty("ORIENTDB_HOME", tmpDir.getAbsolutePath());

		server = OServerMain.create();

		OServerConfiguration config = new OServerConfiguration();

		config.users = new OServerUserConfiguration[2];
		config.users[0] = new OServerUserConfiguration();
		config.users[0].name = USERNAME;
		config.users[0].password = PASSWORD;
		config.users[0].resources = "*";

		config.users[1] = new OServerUserConfiguration();
		config.users[1].name = "guest";
		config.users[1].password = "guest";
		config.users[1].resources = "connect,server.listDatabases,server.dblist";

		config.network = new OServerNetworkConfiguration();
		config.network.protocols = new ArrayList<>();

		OServerNetworkProtocolConfiguration binProtocol = new OServerNetworkProtocolConfiguration("binary", "com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary");
		config.network.protocols.add(binProtocol);

		OServerNetworkProtocolConfiguration httpProtocol = new OServerNetworkProtocolConfiguration("http", "com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpDb");
		config.network.protocols.add(httpProtocol);

		config.network.listeners = new ArrayList<>();
		OServerNetworkListenerConfiguration binListener = new OServerNetworkListenerConfiguration();
		binListener.protocol = "binary";
		binListener.ipAddress = "127.0.0.1";
		binListener.portRange = String.valueOf(binaryPort);

		config.network.listeners.add(binListener);

		OServerNetworkListenerConfiguration httpListener = new OServerNetworkListenerConfiguration();
		httpListener.protocol = "http";
		httpListener.ipAddress = "127.0.0.1";
		httpListener.portRange = String.valueOf(httpPort);

		httpListener.commands = new OServerCommandConfiguration[1];
		httpListener.commands[0] = new OServerCommandConfiguration();
		httpListener.commands[0].implementation = "com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetStaticContent";
		httpListener.commands[0].pattern = "GET|www GET|studio/ GET| GET|*.htm GET|*.html GET|*.xml GET|*.jpeg GET|*.jpg GET|*.png GET|*.gif GET|*.js GET|*.css GET|*.swf GET|*.ico GET|*.txt GET|*.otf GET|*.pjs GET|*.svg";

		httpListener.commands[0].parameters = new OServerEntryConfiguration[2];
		httpListener.commands[0].parameters[0] = new OServerEntryConfiguration("http.cache:*.htm *.html", "Cache-Control: no-cache, no-store, max-age=0, must-revalidate\r\nPragma: no-cache");
		httpListener.commands[0].parameters[1] = new OServerEntryConfiguration("http.cache:default", "Cache-Control: max-age=120");

		config.network.listeners.add(httpListener);

		server.startup(config);

		server.activate();
	}

	@Before
	public void initData() {

		logger.info("HTTP Port: {}", httpPort);
		logger.info("Binary Port: {}", binaryPort);

		String serverUrl = String.format("remote:127.0.0.1:%s/temp", binaryPort);

		logger.info("Connect to remote server {}", serverUrl);

		if (server.existsDatabase("temp")) {
			server.dropDatabase("temp");
		}
		server.createDatabase("temp", ODatabaseType.PLOCAL, null);

		graph = OrientGraph.open(serverUrl, USERNAME, PASSWORD);

		graph.begin();

		Vertex root = graph.addVertex(T.label, "example", "name", "root", "key", ROOT_KEY);

		Vertex root2 = graph.addVertex(T.label, "example", "name", "other_node", "key", LINK_KEY);

		root2.addEdge("contains", root);

		graph.commit();
	}

	@Test
	public void test() throws Exception {

		GraphTraversalSource g = null;

		try {
			g = graph.traversal();

			/*
			 * Set Breakpoint here and copy the studio url from the console
			 */

			logger.info("Vertex count: {}", g.V().count().next());

			g.V().has("key", ROOT_KEY).forEachRemaining(v -> logger.info("Found Vertex: {}", v));
		} finally {
			if (g != null) {
				g.close();
			}
		}
	}

	@Test
	public void testGremlinProperties() throws Exception {
		GraphTraversalSource g = null;

		try {
			g = graph.traversal();

			g.V().has("key", ROOT_KEY).property("testproperty", "testvalue");

			g.V().has("key", ROOT_KEY).forEachRemaining(e -> assertEquals("testvalue", e.property("testproperty")));

		} finally {
			if (g != null) {
				g.close();
			}
		}
	}

	@After
	public void closeDbHandle() {
		if (graph != null && !graph.isClosed()) {
			graph.close();
			graph = null;
		}
	}

	@AfterClass
	public static void shutdownSystem() {
		if (null != server) {
			// Close the database
			server.shutdown();
		}
	}
}
