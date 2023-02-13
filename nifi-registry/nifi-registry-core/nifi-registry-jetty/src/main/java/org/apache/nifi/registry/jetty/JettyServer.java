/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.registry.jetty;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.jetty.configuration.connector.ServerConnectorFactory;
import org.apache.nifi.registry.jetty.connector.ApplicationServerConnectorFactory;
import org.apache.nifi.registry.jetty.headers.ContentSecurityPolicyFilter;
import org.apache.nifi.registry.jetty.headers.StrictTransportSecurityFilter;
import org.apache.nifi.registry.jetty.headers.XFrameOptionsFilter;
import org.apache.nifi.registry.jetty.headers.XSSProtectionFilter;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.security.crypto.CryptoKeyProvider;
import org.eclipse.jetty.annotations.AnnotationConfiguration;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.webapp.Configuration;
import org.eclipse.jetty.webapp.JettyWebXmlConfiguration;
import org.eclipse.jetty.webapp.WebAppClassLoader;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;


public class JettyServer {

    private static final Logger logger = LoggerFactory.getLogger(JettyServer.class);
    private static final String WEB_DEFAULTS_XML = "org/apache/nifi-registry/web/webdefault.xml";

    private static final String ALL_PATHS = "/*";

    private static final FileFilter WAR_FILTER = pathname -> {
        final String nameToTest = pathname.getName().toLowerCase();
        return nameToTest.endsWith(".war") && pathname.isFile();
    };

    private final NiFiRegistryProperties properties;
    private final CryptoKeyProvider masterKeyProvider;
    private final String docsLocation;
    private final Server server;

    private WebAppContext webApiContext;

    public JettyServer(final NiFiRegistryProperties properties, final CryptoKeyProvider cryptoKeyProvider, final String docsLocation) {
        final QueuedThreadPool threadPool = new QueuedThreadPool(properties.getWebThreads());
        threadPool.setName("NiFi Registry Web Server");

        this.properties = properties;
        this.masterKeyProvider = cryptoKeyProvider;
        this.docsLocation = docsLocation;
        this.server = new Server(threadPool);

        // enable the annotation based configuration to ensure the jsp container is initialized properly
        final Configuration.ClassList classlist = Configuration.ClassList.setServerDefault(server);
        classlist.addBefore(JettyWebXmlConfiguration.class.getName(), AnnotationConfiguration.class.getName());

        try {
            configureConnectors();
            loadWars();
        } catch (final Throwable t) {
            startUpFailure(t);
        }
    }

    /**
     * Instantiates this object but does not perform any configuration. Used for unit testing.
     */
    JettyServer(Server server, NiFiRegistryProperties properties) {
        this.server = server;
        this.properties = properties;
        this.masterKeyProvider = null;
        this.docsLocation = null;
    }

    /**
     * Returns a File object for the directory containing NIFI documentation.
     * <p>
     * Formerly, if the docsDirectory did not exist NIFI would fail to start
     * with an IllegalStateException and a rather unhelpful log message.
     * NIFI-2184 updates the process such that if the docsDirectory does not
     * exist an attempt will be made to create the directory. If that is
     * successful NIFI will no longer fail and will start successfully barring
     * any other errors. The side effect of the docsDirectory not being present
     * is that the documentation links under the 'General' portion of the help
     * page will not be accessible, but at least the process will be running.
     *
     * @param docsDirectory Name of documentation directory in installation directory.
     * @return A File object to the documentation directory; else startUpFailure called.
     */
    private File getDocsDir(final String docsDirectory) {
        File docsDir;
        try {
            docsDir = Paths.get(docsDirectory).toRealPath().toFile();
        } catch (IOException ex) {
            logger.info("Directory '" + docsDirectory + "' is missing. Some documentation will be unavailable.");
            docsDir = new File(docsDirectory).getAbsoluteFile();
            final boolean made = docsDir.mkdirs();
            if (!made) {
                logger.error("Failed to create 'docs' directory!");
                startUpFailure(new IOException(docsDir.getAbsolutePath() + " could not be created"));
            }
        }
        return docsDir;
    }

    private void configureConnectors() {
        final ServerConnectorFactory serverConnectorFactory = new ApplicationServerConnectorFactory(server, properties);
        final Set<String> interfaceNames = properties.isHTTPSConfigured() ? properties.getHttpsNetworkInterfaceNames() : Collections.emptySet();
        if (interfaceNames.isEmpty()) {
            final ServerConnector serverConnector = serverConnectorFactory.getServerConnector();
            server.addConnector(serverConnector);
        } else {
            interfaceNames.stream()
                    // Map interface name properties to Network Interfaces
                    .map(interfaceName -> {
                        try {
                            return NetworkInterface.getByName(interfaceName);
                        } catch (final SocketException e) {
                            throw new UncheckedIOException(String.format("Network Interface [%s] not found", interfaceName), e);
                        }
                    })
                    // Map Network Interfaces to host addresses
                    .filter(Objects::nonNull)
                    .flatMap(networkInterface -> Collections.list(networkInterface.getInetAddresses()).stream())
                    .map(InetAddress::getHostAddress)
                    // Map host addresses to Server Connectors
                    .map(host -> {
                        final ServerConnector serverConnector = serverConnectorFactory.getServerConnector();
                        serverConnector.setHost(host);
                        return serverConnector;
                    })
                    .forEach(server::addConnector);
        }
    }

    private void loadWars() throws IOException {
        final File warDirectory = properties.getWarLibDirectory();
        final File[] wars = warDirectory.listFiles(WAR_FILTER);

        if (wars == null) {
            throw new RuntimeException("Unable to access war lib directory: " + warDirectory);
        }

        File webUiWar = null;
        File webApiWar = null;
        File webDocsWar = null;
        for (final File war : wars) {
            if (war.getName().startsWith("nifi-registry-web-ui")) {
                webUiWar = war;
            } else if (war.getName().startsWith("nifi-registry-web-api")) {
                webApiWar = war;
            } else if (war.getName().startsWith("nifi-registry-web-docs")) {
                webDocsWar = war;
            }
        }

        if (webUiWar == null) {
            throw new IllegalStateException("Unable to locate NiFi Registry Web UI");
        } else if (webApiWar == null) {
            throw new IllegalStateException("Unable to locate NiFi Registry Web API");
        } else if (webDocsWar == null) {
            throw new IllegalStateException("Unable to locate NiFi Registry Web Docs");
        }

        WebAppContext webUiContext = loadWar(webUiWar, "/nifi-registry");
        webUiContext.getInitParams().put("oidc-supported", String.valueOf(properties.isOidcEnabled()));

        webApiContext = loadWar(webApiWar, "/nifi-registry-api", getWebApiAdditionalClasspath());
        logger.info("Adding {} object to ServletContext with key 'nifi-registry.properties'", properties.getClass().getSimpleName());
        webApiContext.setAttribute("nifi-registry.properties", properties);
        logger.info("Adding {} object to ServletContext with key 'nifi-registry.key'", masterKeyProvider.getClass().getSimpleName());
        webApiContext.setAttribute("nifi-registry.key", masterKeyProvider);

        // there is an issue scanning the asm repackaged jar so narrow down what we are scanning
        webApiContext.setAttribute("org.eclipse.jetty.server.webapp.WebInfIncludeJarPattern", ".*/spring-[^/]*\\.jar$");

        final String docsContextPath = "/nifi-registry-docs";
        WebAppContext webDocsContext = loadWar(webDocsWar, docsContextPath);
        addDocsServlets(webDocsContext);

        final HandlerCollection handlers = new HandlerCollection();
        handlers.addHandler(webUiContext);
        handlers.addHandler(webApiContext);
        handlers.addHandler(webDocsContext);
        server.setHandler(handlers);
    }

    private WebAppContext loadWar(final File warFile, final String contextPath)
            throws IOException {
        return loadWar(warFile, contextPath, new URL[0]);
    }

    private WebAppContext loadWar(final File warFile, final String contextPath, final URL[] additionalResources)
            throws IOException {
        final WebAppContext webappContext = new WebAppContext(warFile.getPath(), contextPath);
        webappContext.setContextPath(contextPath);
        webappContext.setDisplayName(contextPath);

        // remove slf4j server class to allow WAR files to have slf4j dependencies in WEB-INF/lib
        List<String> serverClasses = new ArrayList<>(Arrays.asList(webappContext.getServerClasses()));
        serverClasses.remove("org.slf4j.");
        webappContext.setServerClasses(serverClasses.toArray(new String[0]));
        webappContext.setDefaultsDescriptor(WEB_DEFAULTS_XML);

        // get the temp directory for this webapp
        final File webWorkingDirectory = properties.getWebWorkingDirectory();
        final File tempDir = new File(webWorkingDirectory, warFile.getName());
        if (tempDir.exists() && !tempDir.isDirectory()) {
            throw new RuntimeException(tempDir.getAbsolutePath() + " is not a directory");
        } else if (!tempDir.exists()) {
            final boolean made = tempDir.mkdirs();
            if (!made) {
                throw new RuntimeException(tempDir.getAbsolutePath() + " could not be created");
            }
        }
        if (!(tempDir.canRead() && tempDir.canWrite())) {
            throw new RuntimeException(tempDir.getAbsolutePath() + " directory does not have read/write privilege");
        }

        // configure the temp dir
        webappContext.setTempDirectory(tempDir);

        // configure the max form size (3x the default)
        webappContext.setMaxFormContentSize(600000);

        // add HTTP security headers to all responses
        ArrayList<Class<? extends Filter>> filters = new ArrayList<>(Arrays.asList(XFrameOptionsFilter.class, ContentSecurityPolicyFilter.class, XSSProtectionFilter.class));
        if (properties.isHTTPSConfigured()) {
            filters.add(StrictTransportSecurityFilter.class);
        }

        filters.forEach( (filter) -> addFilters(filter, webappContext));

        // start out assuming the system ClassLoader will be the parent, but if additional resources were specified then
        // inject a new ClassLoader in between the system and webapp ClassLoaders that contains the additional resources
        ClassLoader parentClassLoader = ClassLoader.getSystemClassLoader();
        if (additionalResources != null && additionalResources.length > 0) {
            parentClassLoader = new URLClassLoader(additionalResources, ClassLoader.getSystemClassLoader());
        }

        webappContext.setClassLoader(new WebAppClassLoader(parentClassLoader, webappContext));

        logger.info("Loading WAR: " + warFile.getAbsolutePath() + " with context path set to " + contextPath);
        return webappContext;
    }

    private void addFilters(Class<? extends Filter> clazz, final WebAppContext webappContext) {
        FilterHolder holder = new FilterHolder(clazz);
        holder.setName(clazz.getSimpleName());
        webappContext.addFilter(holder, ALL_PATHS, EnumSet.allOf(DispatcherType.class));
    }

    private URL[] getWebApiAdditionalClasspath() {
        final String dbDriverDir = properties.getDatabaseDriverDirectory();

        if (StringUtils.isBlank(dbDriverDir)) {
            logger.info("No database driver directory was specified");
            return new URL[0];
        }

        final File dirFile = new File(dbDriverDir);

        if (!dirFile.exists()) {
            logger.warn("Skipping database driver directory that does not exist: " + dbDriverDir);
            return new URL[0];
        }

        if (!dirFile.canRead()) {
            logger.warn("Skipping database driver directory that can not be read: " + dbDriverDir);
            return new URL[0];
        }

        final List<URL> resources = new LinkedList<>();
        try {
            resources.add(dirFile.toURI().toURL());
        } catch (final MalformedURLException mfe) {
            logger.warn("Unable to add {} to classpath due to {}", new Object[]{ dirFile.getAbsolutePath(), mfe.getMessage()}, mfe);
        }

        if (dirFile.isDirectory()) {
            final File[] files = dirFile.listFiles();
            if (files != null) {
                for (final File resource : files) {
                    if (resource.isDirectory()) {
                        logger.warn("Recursive directories are not supported, skipping " + resource.getAbsolutePath());
                    } else {
                        try {
                            resources.add(resource.toURI().toURL());
                        } catch (final MalformedURLException mfe) {
                            logger.warn("Unable to add {} to classpath due to {}", new Object[]{ resource.getAbsolutePath(), mfe.getMessage()}, mfe);
                        }
                    }
                }
            }
        }

        if (!resources.isEmpty()) {
            logger.info("Added additional resources to nifi-registry-api classpath: [");
            for (URL resource : resources) {
                logger.info(" " + resource.toString());
            }
            logger.info("]");
        }

        return resources.toArray(new URL[0]);
    }

    private void addDocsServlets(WebAppContext docsContext) {
        try {
            // Load the nifi-registry/docs directory
            final File docsDir = getDocsDir(docsLocation);

            // Create the servlet which will serve the static resources
            ServletHolder defaultHolder = new ServletHolder("default", DefaultServlet.class);
            defaultHolder.setInitParameter("dirAllowed", "false");

            ServletHolder docs = new ServletHolder("docs", DefaultServlet.class);
            docs.setInitParameter("resourceBase", docsDir.getPath());
            docs.setInitParameter("dirAllowed", "false");

            docsContext.addServlet(docs, "/html/*");
            docsContext.addServlet(defaultHolder, "/");

            // load the rest documentation
            final File webApiDocsDir = new File(webApiContext.getTempDirectory(), "webapp/docs");
            if (!webApiDocsDir.exists()) {
                final boolean made = webApiDocsDir.mkdirs();
                if (!made) {
                    throw new RuntimeException(webApiDocsDir.getAbsolutePath() + " could not be created");
                }
            }

            ServletHolder apiDocs = new ServletHolder("apiDocs", DefaultServlet.class);
            apiDocs.setInitParameter("resourceBase", webApiDocsDir.getPath());
            apiDocs.setInitParameter("dirAllowed", "false");

            docsContext.addServlet(apiDocs, "/rest-api/*");

            logger.info("Loading documents web app with context path set to " + docsContext.getContextPath());

        } catch (Exception ex) {
            logger.error("Unhandled Exception in createDocsWebApp: " + ex.getMessage());
            startUpFailure(ex);
        }
    }

    public void start() {
        try {
            // start the server
            server.start();

            // ensure everything started successfully
            for (Handler handler : server.getChildHandlers()) {
                // see if the handler is a web app
                if (handler instanceof WebAppContext) {
                    WebAppContext context = (WebAppContext) handler;

                    // see if this webapp had any exceptions that would
                    // cause it to be unavailable
                    if (context.getUnavailableException() != null) {
                        startUpFailure(context.getUnavailableException());
                    }
                }
            }

            dumpUrls();
        } catch (final Throwable t) {
            startUpFailure(t);
        }
    }

    private void startUpFailure(Throwable t) {
        System.err.println("Failed to start web server: " + t.getMessage());
        System.err.println("Shutting down...");
        logger.warn("Failed to start web server... shutting down.", t);
        System.exit(1);
    }

    private void dumpUrls() throws SocketException {
        final List<String> urls = new ArrayList<>();

        for (Connector connector : server.getConnectors()) {
            if (connector instanceof ServerConnector) {
                final ServerConnector serverConnector = (ServerConnector) connector;

                Set<String> hosts = new HashSet<>();

                // determine the hosts
                if (StringUtils.isNotBlank(serverConnector.getHost())) {
                    hosts.add(serverConnector.getHost());
                } else {
                    Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
                    if (networkInterfaces != null) {
                        for (NetworkInterface networkInterface : Collections.list(networkInterfaces)) {
                            for (InetAddress inetAddress : Collections.list(networkInterface.getInetAddresses())) {
                                hosts.add(inetAddress.getHostAddress());
                            }
                        }
                    }
                }

                // ensure some hosts were found
                if (!hosts.isEmpty()) {
                    String scheme = "http";
                    if (properties.getSslPort() != null && serverConnector.getPort() == properties.getSslPort()) {
                        scheme = "https";
                    }

                    // dump each url
                    for (String host : hosts) {
                        urls.add(String.format("%s://%s:%s", scheme, host, serverConnector.getPort()));
                    }
                }
            }
        }

        if (urls.isEmpty()) {
            logger.warn("NiFi Registry has started, but the UI is not available on any hosts. Please verify the host properties.");
        } else {
            // log the ui location
            logger.info("NiFi Registry has started. The UI is available at the following URLs:");
            for (final String url : urls) {
                logger.info(String.format("%s/nifi-registry", url));
            }
        }
    }

    public void stop() {
        try {
            server.stop();
        } catch (Exception ex) {
            logger.warn("Failed to stop web server", ex);
        }
    }
}
