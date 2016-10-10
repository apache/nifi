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
package org.apache.nifi.web.server;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.NiFiServer;
import org.apache.nifi.controller.UninheritableFlowException;
import org.apache.nifi.controller.serialization.FlowSerializationException;
import org.apache.nifi.controller.serialization.FlowSynchronizationException;
import org.apache.nifi.lifecycle.LifeCycleStartException;
import org.apache.nifi.nar.ExtensionMapping;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.ui.extension.UiExtension;
import org.apache.nifi.ui.extension.UiExtensionMapping;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.ContentAccess;
import org.apache.nifi.web.NiFiWebConfigurationContext;
import org.apache.nifi.web.UiExtensionType;
import org.eclipse.jetty.annotations.AnnotationConfiguration;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.resource.ResourceCollection;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.webapp.Configuration;
import org.eclipse.jetty.webapp.JettyWebXmlConfiguration;
import org.eclipse.jetty.webapp.WebAppClassLoader;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import javax.servlet.DispatcherType;
import javax.servlet.ServletContext;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Encapsulates the Jetty instance.
 */
public class JettyServer implements NiFiServer {

    private static final Logger logger = LoggerFactory.getLogger(JettyServer.class);
    private static final String WEB_DEFAULTS_XML = "org/apache/nifi/web/webdefault.xml";
    private static final int HEADER_BUFFER_SIZE = 16 * 1024; // 16kb

    private static final FileFilter WAR_FILTER = new FileFilter() {
        @Override
        public boolean accept(File pathname) {
            final String nameToTest = pathname.getName().toLowerCase();
            return nameToTest.endsWith(".war") && pathname.isFile();
        }
    };

    private final Server server;
    private final NiFiProperties props;

    private ExtensionMapping extensionMapping;
    private WebAppContext webApiContext;
    private WebAppContext webDocsContext;

    // content viewer and mime type specific extensions
    private WebAppContext webContentViewerContext;
    private Collection<WebAppContext> contentViewerWebContexts;

    // component (processor, controller service, reporting task) ui extensions
    private UiExtensionMapping componentUiExtensions;
    private Collection<WebAppContext> componentUiExtensionWebContexts;

    /**
     * Creates and configures a new Jetty instance.
     *
     * @param props the configuration
     */
    public JettyServer(final NiFiProperties props) {
        final QueuedThreadPool threadPool = new QueuedThreadPool(props.getWebThreads());
        threadPool.setName("NiFi Web Server");

        // create the server
        this.server = new Server(threadPool);
        this.props = props;

        // enable the annotation based configuration to ensure the jsp container is initialized properly
        final Configuration.ClassList classlist = Configuration.ClassList.setServerDefault(server);
        classlist.addBefore(JettyWebXmlConfiguration.class.getName(), AnnotationConfiguration.class.getName());

        // configure server
        configureConnectors(server);

        // load wars from the nar working directories
        loadWars(locateNarWorkingDirectories());
    }

    private Set<File> locateNarWorkingDirectories() {
        final File frameworkWorkingDir = props.getFrameworkWorkingDirectory();
        final File extensionsWorkingDir = props.getExtensionsWorkingDirectory();

        final File[] frameworkDir = frameworkWorkingDir.listFiles();
        if (frameworkDir == null) {
            throw new IllegalStateException(String.format("Unable to access framework working directory: %s", frameworkWorkingDir.getAbsolutePath()));
        }

        final File[] extensionDirs = extensionsWorkingDir.listFiles();
        if (extensionDirs == null) {
            throw new IllegalStateException(String.format("Unable to access extensions working directory: %s", extensionsWorkingDir.getAbsolutePath()));
        }

        // we want to consider the framework and all extension NARs
        final Set<File> narWorkingDirectories = new HashSet<>(Arrays.asList(frameworkDir));
        narWorkingDirectories.addAll(Arrays.asList(extensionDirs));

        return narWorkingDirectories;
    }

    /**
     * Loads the WARs in the specified NAR working directories. A WAR file must
     * have a ".war" extension.
     *
     * @param narWorkingDirectories dirs
     */
    private void loadWars(final Set<File> narWorkingDirectories) {

        // load WARs
        Map<File, File> warToNarWorkingDirectoryLookup = findWars(narWorkingDirectories);

        // locate each war being deployed
        File webUiWar = null;
        File webApiWar = null;
        File webErrorWar = null;
        File webDocsWar = null;
        File webContentViewerWar = null;
        List<File> otherWars = new ArrayList<>();
        for (File war : warToNarWorkingDirectoryLookup.keySet()) {
            if (war.getName().toLowerCase().startsWith("nifi-web-api")) {
                webApiWar = war;
            } else if (war.getName().toLowerCase().startsWith("nifi-web-error")) {
                webErrorWar = war;
            } else if (war.getName().toLowerCase().startsWith("nifi-web-docs")) {
                webDocsWar = war;
            } else if (war.getName().toLowerCase().startsWith("nifi-web-content-viewer")) {
                webContentViewerWar = war;
            } else if (war.getName().toLowerCase().startsWith("nifi-web")) {
                webUiWar = war;
            } else {
                otherWars.add(war);
            }
        }

        // ensure the required wars were found
        if (webUiWar == null) {
            throw new RuntimeException("Unable to load nifi-web WAR");
        } else if (webApiWar == null) {
            throw new RuntimeException("Unable to load nifi-web-api WAR");
        } else if (webDocsWar == null) {
            throw new RuntimeException("Unable to load nifi-web-docs WAR");
        } else if (webErrorWar == null) {
            throw new RuntimeException("Unable to load nifi-web-error WAR");
        } else if (webContentViewerWar == null) {
            throw new RuntimeException("Unable to load nifi-web-content-viewer WAR");
        }

        // handlers for each war and init params for the web api
        final HandlerCollection handlers = new HandlerCollection();
        final Map<String, String> mimeMappings = new HashMap<>();
        final ClassLoader frameworkClassLoader = getClass().getClassLoader();
        final ClassLoader jettyClassLoader = frameworkClassLoader.getParent();

        // deploy the other wars
        if (CollectionUtils.isNotEmpty(otherWars)) {
            // hold onto to the web contexts for all ui extensions
            componentUiExtensionWebContexts = new ArrayList<>();
            contentViewerWebContexts = new ArrayList<>();

            // ui extension organized by component type
            final Map<String, List<UiExtension>> componentUiExtensionsByType = new HashMap<>();
            for (File war : otherWars) {
                // identify all known extension types in the war
                final Map<UiExtensionType, List<String>> uiExtensionInWar = new HashMap<>();
                identifyUiExtensionsForComponents(uiExtensionInWar, war);

                // only include wars that are for custom processor ui's
                if (!uiExtensionInWar.isEmpty()) {
                    // get the context path
                    String warName = StringUtils.substringBeforeLast(war.getName(), ".");
                    String warContextPath = String.format("/%s", warName);

                    // attempt to locate the nar class loader for this war
                    ClassLoader narClassLoaderForWar = NarClassLoaders.getInstance().getExtensionClassLoader(warToNarWorkingDirectoryLookup.get(war));

                    // this should never be null
                    if (narClassLoaderForWar == null) {
                        narClassLoaderForWar = jettyClassLoader;
                    }

                    // create the extension web app context
                    WebAppContext extensionUiContext = loadWar(war, warContextPath, narClassLoaderForWar);

                    // create the ui extensions
                    for (final Map.Entry<UiExtensionType, List<String>> entry : uiExtensionInWar.entrySet()) {
                        final UiExtensionType extensionType = entry.getKey();
                        final List<String> types = entry.getValue();

                        if (UiExtensionType.ContentViewer.equals(extensionType)) {
                            // consider each content type identified
                            for (final String contentType : types) {
                                // map the content type to the context path
                                mimeMappings.put(contentType, warContextPath);
                            }

                            // this ui extension provides a content viewer
                            contentViewerWebContexts.add(extensionUiContext);
                        } else {
                            // consider each component type identified
                            for (final String componentType : types) {
                                logger.info(String.format("Loading UI extension [%s, %s] for %s", extensionType, warContextPath, types));

                                // record the extension definition
                                final UiExtension uiExtension = new UiExtension(extensionType, warContextPath);

                                // create if this is the first extension for this component type
                                List<UiExtension> componentUiExtensionsForType = componentUiExtensionsByType.get(componentType);
                                if (componentUiExtensionsForType == null) {
                                    componentUiExtensionsForType = new ArrayList<>();
                                    componentUiExtensionsByType.put(componentType, componentUiExtensionsForType);
                                }

                                // record this extension
                                componentUiExtensionsForType.add(uiExtension);
                            }

                            // this ui extension provides a component custom ui
                            componentUiExtensionWebContexts.add(extensionUiContext);
                        }
                    }

                    // include custom ui web context in the handlers
                    handlers.addHandler(extensionUiContext);
                }

            }

            // record all ui extensions to give to the web api
            componentUiExtensions = new UiExtensionMapping(componentUiExtensionsByType);
        } else {
            componentUiExtensions = new UiExtensionMapping(Collections.EMPTY_MAP);
        }

        // load the web ui app
        handlers.addHandler(loadWar(webUiWar, "/nifi", frameworkClassLoader));

        // load the web api app
        webApiContext = loadWar(webApiWar, "/nifi-api", frameworkClassLoader);
        handlers.addHandler(webApiContext);

        // load the content viewer app
        webContentViewerContext = loadWar(webContentViewerWar, "/nifi-content-viewer", frameworkClassLoader);
        webContentViewerContext.getInitParams().putAll(mimeMappings);
        handlers.addHandler(webContentViewerContext);

        // create a web app for the docs
        final String docsContextPath = "/nifi-docs";

        // load the documentation war
        webDocsContext = loadWar(webDocsWar, docsContextPath, frameworkClassLoader);

        // overlay the actual documentation
        final ContextHandlerCollection documentationHandlers = new ContextHandlerCollection();
        documentationHandlers.addHandler(createDocsWebApp(docsContextPath));
        documentationHandlers.addHandler(webDocsContext);
        handlers.addHandler(documentationHandlers);

        // load the web error app
        handlers.addHandler(loadWar(webErrorWar, "/", frameworkClassLoader));

        // deploy the web apps
        server.setHandler(gzip(handlers));
    }

    /**
     * Enables compression for the specified handler.
     *
     * @param handler handler to enable compression for
     * @return compression enabled handler
     */
    private Handler gzip(final Handler handler) {
        final GzipHandler gzip = new GzipHandler();
        gzip.setIncludedMethods("GET", "POST", "PUT", "DELETE");
        gzip.setHandler(handler);
        return gzip;
    }

    private Map<File, File> findWars(final Set<File> narWorkingDirectories) {
        final Map<File, File> wars = new HashMap<>();

        // consider each nar working directory
        for (final File narWorkingDirectory : narWorkingDirectories) {
            final File narDependencies = new File(narWorkingDirectory, "META-INF/bundled-dependencies");
            if (narDependencies.isDirectory()) {
                // list the wars from this nar
                final File[] narDependencyDirs = narDependencies.listFiles(WAR_FILTER);
                if (narDependencyDirs == null) {
                    throw new IllegalStateException(String.format("Unable to access working directory for NAR dependencies in: %s", narDependencies.getAbsolutePath()));
                }

                // add each war
                for (final File war : narDependencyDirs) {
                    wars.put(war, narWorkingDirectory);
                }
            }
        }

        return wars;
    }

    private void readUiExtensions(final Map<UiExtensionType, List<String>> uiExtensions, final UiExtensionType uiExtensionType, final JarFile jarFile, final JarEntry jarEntry) throws IOException {
        if (jarEntry == null) {
            return;
        }

        // get an input stream for the nifi-processor configuration file
        try (BufferedReader in = new BufferedReader(new InputStreamReader(jarFile.getInputStream(jarEntry)))) {

            // read in each configured type
            String rawComponentType;
            while ((rawComponentType = in.readLine()) != null) {
                // extract the component type
                final String componentType = extractComponentType(rawComponentType);
                if (componentType != null) {
                    List<String> extensions = uiExtensions.get(uiExtensionType);

                    // if there are currently no extensions for this type create it
                    if (extensions == null) {
                        extensions = new ArrayList<>();
                        uiExtensions.put(uiExtensionType, extensions);
                    }

                    // add the specified type
                    extensions.add(componentType);
                }
            }
        }
    }

    /**
     * Identifies all known UI extensions and stores them in the specified map.
     *
     * @param uiExtensions extensions
     * @param warFile war
     */
    private void identifyUiExtensionsForComponents(final Map<UiExtensionType, List<String>> uiExtensions, final File warFile) {
        try (final JarFile jarFile = new JarFile(warFile)) {
            // locate the ui extensions
            readUiExtensions(uiExtensions, UiExtensionType.ContentViewer, jarFile, jarFile.getJarEntry("META-INF/nifi-content-viewer"));
            readUiExtensions(uiExtensions, UiExtensionType.ProcessorConfiguration, jarFile, jarFile.getJarEntry("META-INF/nifi-processor-configuration"));
            readUiExtensions(uiExtensions, UiExtensionType.ControllerServiceConfiguration, jarFile, jarFile.getJarEntry("META-INF/nifi-controller-service-configuration"));
            readUiExtensions(uiExtensions, UiExtensionType.ReportingTaskConfiguration, jarFile, jarFile.getJarEntry("META-INF/nifi-reporting-task-configuration"));
        } catch (IOException ioe) {
            logger.warn(String.format("Unable to inspect %s for a UI extensions.", warFile));
        }
    }

    /**
     * Extracts the component type. Trims the line and considers comments.
     * Returns null if no type was found.
     *
     * @param line line
     * @return type
     */
    private String extractComponentType(final String line) {
        final String trimmedLine = line.trim();
        if (!trimmedLine.isEmpty() && !trimmedLine.startsWith("#")) {
            final int indexOfPound = trimmedLine.indexOf("#");
            return (indexOfPound > 0) ? trimmedLine.substring(0, indexOfPound) : trimmedLine;
        }
        return null;
    }

    /**
     * Returns the extension in the specified WAR using the specified path.
     *
     * @param war war
     * @param path path
     * @return extensions
     */
    private List<String> getWarExtensions(final File war, final String path) {
        List<String> processorTypes = new ArrayList<>();

        // load the jar file and attempt to find the nifi-processor entry
        JarFile jarFile = null;
        try {
            jarFile = new JarFile(war);
            JarEntry jarEntry = jarFile.getJarEntry(path);

            // ensure the nifi-processor entry was found
            if (jarEntry != null) {
                // get an input stream for the nifi-processor configuration file
                try (final BufferedReader in = new BufferedReader(
                        new InputStreamReader(jarFile.getInputStream(jarEntry)))) {

                    // read in each configured type
                    String rawProcessorType;
                    while ((rawProcessorType = in.readLine()) != null) {
                        // extract the processor type
                        final String processorType = extractComponentType(rawProcessorType);
                        if (processorType != null) {
                            processorTypes.add(processorType);
                        }
                    }
                }
            }
        } catch (IOException ioe) {
            logger.warn("Unable to inspect {} for a custom processor UI.", new Object[]{war, ioe});
        } finally {
            IOUtils.closeQuietly(jarFile);
        }

        return processorTypes;
    }

    private WebAppContext loadWar(final File warFile, final String contextPath, final ClassLoader parentClassLoader) {
        final WebAppContext webappContext = new WebAppContext(warFile.getPath(), contextPath);
        webappContext.setContextPath(contextPath);
        webappContext.setDisplayName(contextPath);

        // instruction jetty to examine these jars for tlds, web-fragments, etc
        webappContext.setAttribute("org.eclipse.jetty.server.webapp.ContainerIncludeJarPattern", ".*/[^/]*servlet-api-[^/]*\\.jar$|.*/javax.servlet.jsp.jstl-.*\\\\.jar$|.*/[^/]*taglibs.*\\.jar$" );

        // remove slf4j server class to allow WAR files to have slf4j dependencies in WEB-INF/lib
        List<String> serverClasses = new ArrayList<>(Arrays.asList(webappContext.getServerClasses()));
        serverClasses.remove("org.slf4j.");
        webappContext.setServerClasses(serverClasses.toArray(new String[0]));
        webappContext.setDefaultsDescriptor(WEB_DEFAULTS_XML);

        // get the temp directory for this webapp
        File tempDir = new File(props.getWebWorkingDirectory(), warFile.getName());
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

        try {
            // configure the class loader - webappClassLoader -> jetty nar -> web app's nar -> ...
            webappContext.setClassLoader(new WebAppClassLoader(parentClassLoader, webappContext));
        } catch (final IOException ioe) {
            startUpFailure(ioe);
        }

        logger.info("Loading WAR: " + warFile.getAbsolutePath() + " with context path set to " + contextPath);
        return webappContext;
    }

    private ContextHandler createDocsWebApp(final String contextPath) {
        try {
            final ResourceHandler resourceHandler = new ResourceHandler();
            resourceHandler.setDirectoriesListed(false);

            // load the docs directory
            final File docsDir = Paths.get("docs").toRealPath().toFile();
            final Resource docsResource = Resource.newResource(docsDir);

            // load the component documentation working directory
            final String componentDocsDirPath = props.getProperty(NiFiProperties.COMPONENT_DOCS_DIRECTORY, "work/docs/components");
            final File workingDocsDirectory = Paths.get(componentDocsDirPath).toRealPath().getParent().toFile();
            final Resource workingDocsResource = Resource.newResource(workingDocsDirectory);

            // load the rest documentation
            final File webApiDocsDir = new File(webApiContext.getTempDirectory(), "webapp/docs");
            if (!webApiDocsDir.exists()) {
                final boolean made = webApiDocsDir.mkdirs();
                if (!made) {
                    throw new RuntimeException(webApiDocsDir.getAbsolutePath() + " could not be created");
                }
            }
            final Resource webApiDocsResource = Resource.newResource(webApiDocsDir);

            // create resources for both docs locations
            final ResourceCollection resources = new ResourceCollection(docsResource, workingDocsResource, webApiDocsResource);
            resourceHandler.setBaseResource(resources);

            // create the context handler
            final ContextHandler handler = new ContextHandler(contextPath);
            handler.setHandler(resourceHandler);

            logger.info("Loading documents web app with context path set to " + contextPath);
            return handler;
        } catch (Exception ex) {
            throw new IllegalStateException("Resource directory paths are malformed: " + ex.getMessage());
        }
    }

    private void configureConnectors(final Server server) throws ServerConfigurationException {
        // create the http configuration
        final HttpConfiguration httpConfiguration = new HttpConfiguration();
        httpConfiguration.setRequestHeaderSize(HEADER_BUFFER_SIZE);
        httpConfiguration.setResponseHeaderSize(HEADER_BUFFER_SIZE);

        if (props.getPort() != null) {
            final Integer port = props.getPort();
            if (port < 0 || (int) Math.pow(2, 16) <= port) {
                throw new ServerConfigurationException("Invalid HTTP port: " + port);
            }

            logger.info("Configuring Jetty for HTTP on port: " + port);

            // create the connector
            final ServerConnector http = new ServerConnector(server, new HttpConnectionFactory(httpConfiguration));

            // set host and port
            if (StringUtils.isNotBlank(props.getProperty(NiFiProperties.WEB_HTTP_HOST))) {
                http.setHost(props.getProperty(NiFiProperties.WEB_HTTP_HOST));
            }
            http.setPort(port);

            // add this connector
            server.addConnector(http);
        }

        if (props.getSslPort() != null) {
            final Integer port = props.getSslPort();
            if (port < 0 || (int) Math.pow(2, 16) <= port) {
                throw new ServerConfigurationException("Invalid HTTPs port: " + port);
            }

            logger.info("Configuring Jetty for HTTPs on port: " + port);

            // add some secure config
            final HttpConfiguration httpsConfiguration = new HttpConfiguration(httpConfiguration);
            httpsConfiguration.setSecureScheme("https");
            httpsConfiguration.setSecurePort(props.getSslPort());
            httpsConfiguration.addCustomizer(new SecureRequestCustomizer());

            // build the connector
            final ServerConnector https = new ServerConnector(server,
                    new SslConnectionFactory(createSslContextFactory(), "http/1.1"),
                    new HttpConnectionFactory(httpsConfiguration));

            // set host and port
            if (StringUtils.isNotBlank(props.getProperty(NiFiProperties.WEB_HTTPS_HOST))) {
                https.setHost(props.getProperty(NiFiProperties.WEB_HTTPS_HOST));
            }
            https.setPort(port);

            // add this connector
            server.addConnector(https);
        }
    }

    private SslContextFactory createSslContextFactory() {
        final SslContextFactory contextFactory = new SslContextFactory();
        configureSslContextFactory(contextFactory, props);
        return contextFactory;
    }

    protected static void configureSslContextFactory(SslContextFactory contextFactory, NiFiProperties props) {
        // require client auth when not supporting login, Kerberos service, or anonymous access
        if (props.isClientAuthRequiredForRestApi()) {
            contextFactory.setNeedClientAuth(true);
        } else {
            contextFactory.setWantClientAuth(true);
        }

        /* below code sets JSSE system properties when values are provided */
        // keystore properties
        if (StringUtils.isNotBlank(props.getProperty(NiFiProperties.SECURITY_KEYSTORE))) {
            contextFactory.setKeyStorePath(props.getProperty(NiFiProperties.SECURITY_KEYSTORE));
        }
        if (StringUtils.isNotBlank(props.getProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE))) {
            contextFactory.setKeyStoreType(props.getProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE));
        }
        final String keystorePassword = props.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD);
        final String keyPassword = props.getProperty(NiFiProperties.SECURITY_KEY_PASSWD);
        if (StringUtils.isNotBlank(keystorePassword)) {
            // if no key password was provided, then assume the keystore password is the same as the key password.
            final String defaultKeyPassword = (StringUtils.isBlank(keyPassword)) ? keystorePassword : keyPassword;
            contextFactory.setKeyStorePassword(keystorePassword);
            contextFactory.setKeyManagerPassword(defaultKeyPassword);
        } else if (StringUtils.isNotBlank(keyPassword)) {
            // since no keystore password was provided, there will be no keystore integrity check
            contextFactory.setKeyManagerPassword(keyPassword);
        }

        // truststore properties
        if (StringUtils.isNotBlank(props.getProperty(NiFiProperties.SECURITY_TRUSTSTORE))) {
            contextFactory.setTrustStorePath(props.getProperty(NiFiProperties.SECURITY_TRUSTSTORE));
        }
        if (StringUtils.isNotBlank(props.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE))) {
            contextFactory.setTrustStoreType(props.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE));
        }
        if (StringUtils.isNotBlank(props.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD))) {
            contextFactory.setTrustStorePassword(props.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD));
        }
    }

    @Override
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

            // ensure the appropriate wars deployed successfully before injecting the NiFi context and security filters
            // this must be done after starting the server (and ensuring there were no start up failures)
            if (webApiContext != null) {
                // give the web api the component ui extensions
                final ServletContext webApiServletContext = webApiContext.getServletHandler().getServletContext();
                webApiServletContext.setAttribute("nifi-ui-extensions", componentUiExtensions);

                // get the application context
                final WebApplicationContext webApplicationContext = WebApplicationContextUtils.getRequiredWebApplicationContext(webApiServletContext);

                // component ui extensions
                if (CollectionUtils.isNotEmpty(componentUiExtensionWebContexts)) {
                    final NiFiWebConfigurationContext configurationContext = webApplicationContext.getBean("nifiWebConfigurationContext", NiFiWebConfigurationContext.class);

                    for (final WebAppContext customUiContext : componentUiExtensionWebContexts) {
                        // set the NiFi context in each custom ui servlet context
                        final ServletContext customUiServletContext = customUiContext.getServletHandler().getServletContext();
                        customUiServletContext.setAttribute("nifi-web-configuration-context", configurationContext);

                        // add the security filter to any ui extensions wars
                        final FilterHolder securityFilter = webApiContext.getServletHandler().getFilter("springSecurityFilterChain");
                        if (securityFilter != null) {
                            customUiContext.addFilter(securityFilter, "/*", EnumSet.allOf(DispatcherType.class));
                        }
                    }
                }

                // content viewer extensions
                if (CollectionUtils.isNotEmpty(contentViewerWebContexts)) {
                    for (final WebAppContext contentViewerContext : contentViewerWebContexts) {
                        // add the security filter to any content viewer  wars
                        final FilterHolder securityFilter = webApiContext.getServletHandler().getFilter("springSecurityFilterChain");
                        if (securityFilter != null) {
                            contentViewerContext.addFilter(securityFilter, "/*", EnumSet.allOf(DispatcherType.class));
                        }
                    }
                }

                // content viewer controller
                if (webContentViewerContext != null) {
                    final ContentAccess contentAccess = webApplicationContext.getBean("contentAccess", ContentAccess.class);

                    // add the content access
                    final ServletContext webContentViewerServletContext = webContentViewerContext.getServletHandler().getServletContext();
                    webContentViewerServletContext.setAttribute("nifi-content-access", contentAccess);

                    final FilterHolder securityFilter = webApiContext.getServletHandler().getFilter("springSecurityFilterChain");
                    if (securityFilter != null) {
                        webContentViewerContext.addFilter(securityFilter, "/*", EnumSet.allOf(DispatcherType.class));
                    }
                }
            }

            // ensure the web document war was loaded and provide the extension mapping
            if (webDocsContext != null) {
                final ServletContext webDocsServletContext = webDocsContext.getServletHandler().getServletContext();
                webDocsServletContext.setAttribute("nifi-extension-mapping", extensionMapping);
            }

            // if this nifi is a node in a cluster, start the flow service and load the flow - the
            // flow service is loaded here for clustered nodes because the loading of the flow will
            // initialize the connection between the node and the NCM. if the node connects (starts
            // heartbeating, etc), the NCM may issue web requests before the application (wars) have
            // finished loading. this results in the node being disconnected since its unable to
            // successfully respond to the requests. to resolve this, flow loading was moved to here
            // (after the wars have been successfully deployed) when this nifi instance is a node
            // in a cluster
            if (props.isNode()) {

                FlowService flowService = null;
                try {

                    logger.info("Loading Flow...");

                    ApplicationContext ctx = WebApplicationContextUtils.getWebApplicationContext(webApiContext.getServletContext());
                    flowService = ctx.getBean("flowService", FlowService.class);

                    // start and load the flow
                    flowService.start();
                    flowService.load(null);

                    logger.info("Flow loaded successfully.");

                } catch (BeansException | LifeCycleStartException | IOException | FlowSerializationException | FlowSynchronizationException | UninheritableFlowException e) {
                    // ensure the flow service is terminated
                    if (flowService != null && flowService.isRunning()) {
                        flowService.stop(false);
                    }
                    throw new Exception("Unable to load flow due to: " + e, e);
                }
            }

            // dump the application url after confirming everything started successfully
            dumpUrls();
        } catch (Exception ex) {
            startUpFailure(ex);
        }
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
                    if (props.getSslPort() != null && serverConnector.getPort() == props.getSslPort()) {
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
            logger.warn("NiFi has started, but the UI is not available on any hosts. Please verify the host properties.");
        } else {
            // log the ui location
            logger.info("NiFi has started. The UI is available at the following URLs:");
            for (final String url : urls) {
                logger.info(String.format("%s/nifi", url));
            }
        }
    }

    private void startUpFailure(Throwable t) {
        System.err.println("Failed to start web server: " + t.getMessage());
        System.err.println("Shutting down...");
        logger.warn("Failed to start web server... shutting down.", t);
        System.exit(1);
    }

    @Override
    public void setExtensionMapping(ExtensionMapping extensionMapping) {
        this.extensionMapping = extensionMapping;
    }

    @Override
    public void stop() {
        try {
            server.stop();
        } catch (Exception ex) {
            logger.warn("Failed to stop web server", ex);
        }
    }
}
