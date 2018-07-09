package example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerEntryConfiguration;
import com.orientechnologies.orient.server.config.OServerNetworkConfiguration;
import com.orientechnologies.orient.server.config.OServerNetworkListenerConfiguration;
import com.orientechnologies.orient.server.config.OServerNetworkProtocolConfiguration;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;

public class GremlinVsHttpTest {
	protected static final Logger logger = LogManager.getLogger(GremlinVsHttpTest.class);
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
	private static UUID LINK2_KEY = UUID.randomUUID();

	static ODatabasePool pool;

	HttpClientContext context;

	ObjectMapper mapper = new ObjectMapper();

	@BeforeClass
	public static void initSystemViaConfig() throws Exception {

		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();

		java.util.logging.Logger orientDbLogger = java.util.logging.Logger.getLogger("com.orientechnologies");
		orientDbLogger.setLevel(Level.WARNING);

		httpPort = ServerRandomPort.getFreePort();
		binaryPort = ServerRandomPort.getFreePort();

		File tmpDir = folder.newFolder("temp");

		URL location = GremlinVsHttpTest.class.getProtectionDomain().getCodeSource().getLocation();
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

		OServerNetworkProtocolConfiguration binProtocol = new OServerNetworkProtocolConfiguration("binary",
				"com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary");
		config.network.protocols.add(binProtocol);

		OServerNetworkProtocolConfiguration httpProtocol = new OServerNetworkProtocolConfiguration("http",
				"com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpDb");
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
		httpListener.commands[0].parameters[0] = new OServerEntryConfiguration("http.cache:*.htm *.html",
				"Cache-Control: no-cache, no-store, max-age=0, must-revalidate\r\nPragma: no-cache");
		httpListener.commands[0].parameters[1] = new OServerEntryConfiguration("http.cache:default",
				"Cache-Control: max-age=120");

		config.network.listeners.add(httpListener);

		server.startup(config);

		server.activate();

		/*
		 * InitData
		 */

		logger.info("HTTP Port: {}", httpPort);
		logger.info("Binary Port: {}", binaryPort);

		String serverUrl = String.format("remote:127.0.0.1:%s/temp", binaryPort);

		logger.info("Connect to remote server {}", serverUrl);

		if (server.existsDatabase("temp")) {
			server.dropDatabase("temp");
		}
		server.createDatabase("temp", ODatabaseType.PLOCAL, null);

		OrientGraph graph = OrientGraph.open(serverUrl, USERNAME, PASSWORD);

		graph.begin();

		Vertex root = graph.addVertex(T.label, "example", "name", "root", "key", ROOT_KEY.toString());

		Vertex link = graph.addVertex(T.label, "example", "name", "other_node", "key", LINK_KEY.toString());

		Vertex link2 = graph.addVertex(T.label, "example2", "name2", "other_node2", "key", LINK2_KEY.toString());

		link2.addEdge("contains", link);

		link.addEdge("contains", root);

		graph.commit();
		graph.close();

		OrientDB orientDB = new OrientDB(serverUrl, USERNAME, PASSWORD, OrientDBConfig.defaultConfig());

		pool = new ODatabasePool(orientDB, "temp", USERNAME, PASSWORD);
	}

	@Before
	public void initConnection() {
		String serverUrl = String.format("remote:127.0.0.1:%s/temp", binaryPort);

		graph = OrientGraph.open(serverUrl, USERNAME, PASSWORD);
	}

	@After
	public void closeConnection() {
		if (null != graph) {
			graph.close();
			graph = null;
		}
	}

	@Test
	public void testGremlinApi() throws Exception {

		GraphTraversalSource g = null;

		try {
			g = graph.traversal();

			/*
			 * Set Breakpoint here and copy the studio url from the console
			 */

			logger.info("Vertex count: {}", g.V().count().next());

			List<Object> data = g.V().has("key", LINK2_KEY.toString()).repeat(__.out().simplePath())
					.until(__.has("key", ROOT_KEY.toString())).path().unfold().toList();

			assertNotNull(data);
		} finally {
			if (g != null) {
				g.close();
			}
		}
	}

	@Test
	public void testGremlinHttpApi() throws ClientProtocolException, IOException {

		CloseableHttpClient client = createClient(new HttpHost("localhost", httpPort, "http"), USERNAME, PASSWORD);

		String query = String.format(
				"g.V().has('key', '%s').repeat(out().simplePath()).until(has('key', '%s')).path().unfold()",
				LINK2_KEY.toString(), ROOT_KEY.toString());

		HashMap<String, String> command = new HashMap<String, String>();
		command.put("command", query);
		// command.put("command", String.format("g.V().has('key',
		// '%s').repeat(__.out().simplePath())", LINK_KEY.toString()));

		String commandJson = mapper.writeValueAsString(command);

		HttpPost post = new HttpPost(String.format("http://%s:%s/command/temp/gremlin", "localhost", httpPort));
		post.setEntity(new StringEntity(commandJson, ContentType.APPLICATION_JSON));

		CloseableHttpResponse response = client.execute(post, this.context);

		assertNotNull(response);

		HttpEntity responseEntity = response.getEntity();

		String entity = EntityUtils.toString(responseEntity);

		assertNotNull(entity);

		LinkedHashMap json = mapper.readValue(entity, LinkedHashMap.class);

		assertNotNull(json);
		assertTrue(json.containsKey("result"));

		assertNotEquals("[]", json.get("result").toString());
	}

	@Test
	public void testGroupCountNativeGremlin() throws Exception {
		GraphTraversalSource g = null;
		try {
			g = graph.traversal();

			Map<Object, Long> data = g.V().has("key", ROOT_KEY.toString()).repeat(__.in("contains").simplePath()).emit()
					.path().by(__.label()).groupCount().next();

			assertNotNull(data);
		} finally {
			if (g != null) {
				g.close();
			}
		}

	}

	@Test
	public void testGroupCount() throws ClientProtocolException, IOException {
		CloseableHttpClient client = createClient(new HttpHost("localhost", httpPort, "http"), USERNAME, PASSWORD);

		// Query from the gitHub Ticket
		String query = String.format(
				"g.V().has('key', '%s').repeat(__.in(\"contains\").simplePath()).emit().path().by(label()).groupCount().toList()",
				ROOT_KEY.toString());

		// Java Api Working Query - but without results
		// String query = String.format("g.V().has('key',
		// '%s').repeat(__.in(\"contains\").simplePath()).emit().path().by(label()).groupCount().next()",
		// ROOT_KEY.toString());

		HashMap<String, String> command = new HashMap<String, String>();
		command.put("command", query);
		// command.put("command", String.format("g.V().has('key',
		// '%s').repeat(__.out().simplePath())", LINK_KEY.toString()));

		String commandJson = mapper.writeValueAsString(command);

		HttpPost post = new HttpPost(String.format("http://%s:%s/command/temp/gremlin", "localhost", httpPort));
		post.setEntity(new StringEntity(commandJson, ContentType.APPLICATION_JSON));

		CloseableHttpResponse response = client.execute(post, this.context);

		assertNotNull(response);

		HttpEntity responseEntity = response.getEntity();

		String entity = EntityUtils.toString(responseEntity);

		assertNotNull(entity);
		
		logger.debug("Test Response: {}", entity);

	}

	CloseableHttpClient createClient(HttpHost httpHost, String user, String password) {
		HttpClientBuilder httpclientBuilder = HttpClientBuilder.create();

		if (null != user && null != password) {
			CredentialsProvider credsProvider = new BasicCredentialsProvider();
			credsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));

			RequestConfig.Builder requestBuilder = RequestConfig.custom();
			requestBuilder = requestBuilder.setAuthenticationEnabled(true);

			httpclientBuilder.setDefaultRequestConfig(requestBuilder.build())
					.setDefaultCredentialsProvider(credsProvider);

			AuthCache authCache = new BasicAuthCache();
			authCache.put(httpHost, new BasicScheme());

			// Add AuthCache to the execution context
			context = HttpClientContext.create();
			context.setCredentialsProvider(credsProvider);
			context.setAuthCache(authCache);

			return httpclientBuilder.build();
		} else {
			return HttpClients.createMinimal();
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
