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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.NiFiServer;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.controller.UninheritableFlowException;
import org.apache.nifi.controller.serialization.FlowSerializationException;
import org.apache.nifi.controller.serialization.FlowSynchronizationException;
import org.apache.nifi.documentation.DocGenerator;
import org.apache.nifi.lifecycle.LifeCycleStartException;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.ExtensionManagerHolder;
import org.apache.nifi.nar.ExtensionMapping;
import org.apache.nifi.nar.ExtensionUiLoader;
import org.apache.nifi.nar.NarAutoLoader;
import org.apache.nifi.nar.NarClassLoadersHolder;
import org.apache.nifi.nar.NarLoader;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.StandardNarLoader;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.ui.extension.UiExtension;
import org.apache.nifi.ui.extension.UiExtensionMapping;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.ContentAccess;
import org.apache.nifi.web.NiFiWebConfigurationContext;
import org.apache.nifi.web.UiExtensionType;
import org.apache.nifi.web.security.ContentSecurityPolicyFilter;
import org.eclipse.jetty.annotations.AnnotationConfiguration;
import org.eclipse.jetty.deploy.App;
import org.eclipse.jetty.deploy.DeploymentManager;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletHolder;
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
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

/**
 * Encapsulates the Jetty instance.
 */
public class JettyServer implements NiFiServer, ExtensionUiLoader {

    private static final Logger logger = LoggerFactory.getLogger(JettyServer.class);
    private static final String WEB_DEFAULTS_XML = "org/apache/nifi/web/webdefault.xml";

    private static final String CONTAINER_INCLUDE_PATTERN_KEY = "org.eclipse.jetty.server.webapp.ContainerIncludeJarPattern";
    private static final String CONTAINER_INCLUDE_PATTERN_VALUE = ".*/[^/]*servlet-api-[^/]*\\.jar$|.*/javax.servlet.jsp.jstl-.*\\\\.jar$|.*/[^/]*taglibs.*\\.jar$";

    private static final FileFilter WAR_FILTER = new FileFilter() {
        @Override
        public boolean accept(File pathname) {
            final String nameToTest = pathname.getName().toLowerCase();
            return nameToTest.endsWith(".war") && pathname.isFile();
        }
    };

    private final Server server;
    private final NiFiProperties props;

    private Bundle systemBundle;
    private Set<Bundle> bundles;
    private ExtensionMapping extensionMapping;
    private NarAutoLoader narAutoLoader;

    private WebAppContext webApiContext;
    private WebAppContext webDocsContext;

    // content viewer and mime type specific extensions
    private WebAppContext webContentViewerContext;
    private Collection<WebAppContext> contentViewerWebContexts;

    // component (processor, controller service, reporting task) ui extensions
    private UiExtensionMapping componentUiExtensions;
    private Collection<WebAppContext> componentUiExtensionWebContexts;

    private DeploymentManager deploymentManager;

    public JettyServer(final NiFiProperties props, final Set<Bundle> bundles) {
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

        // load wars from the bundle
        final Handler warHandlers = loadInitialWars(bundles);

        final HandlerList allHandlers = new HandlerList();

        // Only restrict the host header if running in HTTPS mode
        if (props.isHTTPSConfigured()) {
            // Create a handler for the host header and add it to the server
            HostHeaderHandler hostHeaderHandler = new HostHeaderHandler(props);
            logger.info("Created HostHeaderHandler [" + hostHeaderHandler.toString() + "]");

            // Add this before the WAR handlers
            allHandlers.addHandler(hostHeaderHandler);
        } else {
            logger.info("Running in HTTP mode; host headers not restricted");
        }


        final ContextHandlerCollection contextHandlers = new ContextHandlerCollection();
        contextHandlers.addHandler(warHandlers);
        allHandlers.addHandler(contextHandlers);
        server.setHandler(allHandlers);

        deploymentManager = new DeploymentManager();
        deploymentManager.setContextAttribute(CONTAINER_INCLUDE_PATTERN_KEY, CONTAINER_INCLUDE_PATTERN_VALUE);
        deploymentManager.setContexts(contextHandlers);
        server.addBean(deploymentManager);
    }

    /**
     * Instantiates this object but does not perform any configuration. Used for unit testing.
     */
     JettyServer(Server server, NiFiProperties properties) {
        this.server = server;
        this.props = properties;
    }

    private Handler loadInitialWars(final Set<Bundle> bundles) {

        // load WARs
        final Map<File, Bundle> warToBundleLookup = findWars(bundles);

        // locate each war being deployed
        File webUiWar = null;
        File webApiWar = null;
        File webErrorWar = null;
        File webDocsWar = null;
        File webContentViewerWar = null;
        Map<File, Bundle> otherWars = new HashMap<>();
        for (Map.Entry<File,Bundle> warBundleEntry : warToBundleLookup.entrySet()) {
            final File war = warBundleEntry.getKey();
            final Bundle warBundle = warBundleEntry.getValue();

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
                otherWars.put(war, warBundle);
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
        final ExtensionUiInfo extensionUiInfo = loadWars(otherWars);
        componentUiExtensionWebContexts = new ArrayList<>(extensionUiInfo.getComponentUiExtensionWebContexts());
        contentViewerWebContexts = new ArrayList<>(extensionUiInfo.getContentViewerWebContexts());
        componentUiExtensions = new UiExtensionMapping(extensionUiInfo.getComponentUiExtensionsByType());

        final HandlerCollection webAppContextHandlers = new HandlerCollection();
        final Collection<WebAppContext> extensionUiContexts = extensionUiInfo.getWebAppContexts();
        extensionUiContexts.stream().forEach(c -> webAppContextHandlers.addHandler(c));

        final ClassLoader frameworkClassLoader = getClass().getClassLoader();

        // load the web ui app
        final WebAppContext webUiContext = loadWar(webUiWar, "/nifi", frameworkClassLoader);
        webUiContext.getInitParams().put("oidc-supported", String.valueOf(props.isOidcEnabled()));
        webUiContext.getInitParams().put("knox-supported", String.valueOf(props.isKnoxSsoEnabled()));
        webUiContext.getInitParams().put("whitelistedContextPaths", props.getWhitelistedContextPaths());
        webAppContextHandlers.addHandler(webUiContext);

        // load the web api app
        webApiContext = loadWar(webApiWar, "/nifi-api", frameworkClassLoader);
        webAppContextHandlers.addHandler(webApiContext);

        // load the content viewer app
        webContentViewerContext = loadWar(webContentViewerWar, "/nifi-content-viewer", frameworkClassLoader);
        webContentViewerContext.getInitParams().putAll(extensionUiInfo.getMimeMappings());
        webAppContextHandlers.addHandler(webContentViewerContext);

        // create a web app for the docs
        final String docsContextPath = "/nifi-docs";

        // load the documentation war
        webDocsContext = loadWar(webDocsWar, docsContextPath, frameworkClassLoader);

        // add the servlets which serve the HTML documentation within the documentation web app
        addDocsServlets(webDocsContext);

        webAppContextHandlers.addHandler(webDocsContext);

        // load the web error app
        final WebAppContext webErrorContext = loadWar(webErrorWar, "/", frameworkClassLoader);
        webErrorContext.getInitParams().put("whitelistedContextPaths", props.getWhitelistedContextPaths());
        webAppContextHandlers.addHandler(webErrorContext);

        // deploy the web apps
        return gzip(webAppContextHandlers);
    }

    @Override
    public void loadExtensionUis(final Set<Bundle> bundles) {
         // Find and load any WARs contained within the set of bundles...
        final Map<File, Bundle> warToBundleLookup = findWars(bundles);
        final ExtensionUiInfo extensionUiInfo = loadWars(warToBundleLookup);

        final Collection<WebAppContext> webAppContexts = extensionUiInfo.getWebAppContexts();
        if (CollectionUtils.isEmpty(webAppContexts)) {
            logger.debug("No webapp contexts were loaded, returning...");
            return;
        }

        // Deploy each WAR that was loaded...
        for (final WebAppContext webAppContext : webAppContexts) {
            final App extensionUiApp = new App(deploymentManager, null, "nifi-jetty-server", webAppContext);
            deploymentManager.addApp(extensionUiApp);
        }

        final Collection<WebAppContext> componentUiExtensionWebContexts = extensionUiInfo.getComponentUiExtensionWebContexts();
        final Collection<WebAppContext> contentViewerWebContexts = extensionUiInfo.getContentViewerWebContexts();

        // Inject the configuration context and security filter into contexts that need it
        final ServletContext webApiServletContext = webApiContext.getServletHandler().getServletContext();
        final WebApplicationContext webApplicationContext = WebApplicationContextUtils.getRequiredWebApplicationContext(webApiServletContext);
        final NiFiWebConfigurationContext configurationContext = webApplicationContext.getBean("nifiWebConfigurationContext", NiFiWebConfigurationContext.class);
        final FilterHolder securityFilter = webApiContext.getServletHandler().getFilter("springSecurityFilterChain");

        performInjectionForComponentUis(componentUiExtensionWebContexts, configurationContext, securityFilter);
        performInjectionForContentViewerUis(contentViewerWebContexts, securityFilter);

        // Merge results of current loading into previously loaded results...
        this.componentUiExtensionWebContexts.addAll(componentUiExtensionWebContexts);
        this.contentViewerWebContexts.addAll(contentViewerWebContexts);
        this.componentUiExtensions.addUiExtensions(extensionUiInfo.getComponentUiExtensionsByType());

        for (final WebAppContext webAppContext : webAppContexts) {
            final Throwable t = webAppContext.getUnavailableException();
            if (t != null) {
                logger.error("Unable to start context due to " + t.getMessage(), t);
            }
        }
    }

    private ExtensionUiInfo loadWars(final Map<File, Bundle> warToBundleLookup) {
         // handlers for each war and init params for the web api
        final List<WebAppContext> webAppContexts = new ArrayList<>();
        final Map<String, String> mimeMappings = new HashMap<>();
        final Collection<WebAppContext> componentUiExtensionWebContexts = new ArrayList<>();
        final Collection<WebAppContext> contentViewerWebContexts = new ArrayList<>();
        final Map<String, List<UiExtension>> componentUiExtensionsByType = new HashMap<>();

        final ClassLoader frameworkClassLoader = getClass().getClassLoader();
        final ClassLoader jettyClassLoader = frameworkClassLoader.getParent();

        // deploy the other wars
        if (!warToBundleLookup.isEmpty()) {
            // ui extension organized by component type
            for (Map.Entry<File,Bundle> warBundleEntry : warToBundleLookup.entrySet()) {
                final File war = warBundleEntry.getKey();
                final Bundle warBundle = warBundleEntry.getValue();

                // identify all known extension types in the war
                final Map<UiExtensionType, List<String>> uiExtensionInWar = new HashMap<>();
                identifyUiExtensionsForComponents(uiExtensionInWar, war);

                // only include wars that are for custom processor ui's
                if (!uiExtensionInWar.isEmpty()) {
                    // get the context path
                    String warName = StringUtils.substringBeforeLast(war.getName(), ".");
                    String warContextPath = String.format("/%s", warName);

                    // get the classloader for this war
                    ClassLoader narClassLoaderForWar = warBundle.getClassLoader();

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
                            for (final String componentTypeCoordinates : types) {
                                logger.info(String.format("Loading UI extension [%s, %s] for %s", extensionType, warContextPath, componentTypeCoordinates));

                                // record the extension definition
                                final UiExtension uiExtension = new UiExtension(extensionType, warContextPath);

                                // create if this is the first extension for this component type
                                List<UiExtension> componentUiExtensionsForType = componentUiExtensionsByType.get(componentTypeCoordinates);
                                if (componentUiExtensionsForType == null) {
                                    componentUiExtensionsForType = new ArrayList<>();
                                    componentUiExtensionsByType.put(componentTypeCoordinates, componentUiExtensionsForType);
                                }

                                // see if there is already a ui extension of this same time
                                if (containsUiExtensionType(componentUiExtensionsForType, extensionType)) {
                                    throw new IllegalStateException(String.format("Encountered duplicate UI for %s", componentTypeCoordinates));
                                }

                                // record this extension
                                componentUiExtensionsForType.add(uiExtension);
                            }

                            // this ui extension provides a component custom ui
                            componentUiExtensionWebContexts.add(extensionUiContext);
                        }
                    }

                    // include custom ui web context in the handlers
                    webAppContexts.add(extensionUiContext);
                }
            }
        }

        return new ExtensionUiInfo(webAppContexts, mimeMappings, componentUiExtensionWebContexts, contentViewerWebContexts, componentUiExtensionsByType);
    }

    /**
     * Returns whether or not the specified ui extensions already contains an extension of the specified type.
     *
     * @param componentUiExtensionsForType ui extensions for the type
     * @param extensionType                type of ui extension
     * @return whether or not the specified ui extensions already contains an extension of the specified type
     */
    private boolean containsUiExtensionType(final List<UiExtension> componentUiExtensionsForType, final UiExtensionType extensionType) {
        for (final UiExtension uiExtension : componentUiExtensionsForType) {
            if (extensionType.equals(uiExtension.getExtensionType())) {
                return true;
            }
        }

        return false;
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

    private Map<File, Bundle> findWars(final Set<Bundle> bundles) {
        final Map<File, Bundle> wars = new HashMap<>();

        // consider each nar working directory
        bundles.forEach(bundle -> {
            final BundleDetails details = bundle.getBundleDetails();
            final File narDependencies = new File(details.getWorkingDirectory(), "NAR-INF/bundled-dependencies");
            if (narDependencies.isDirectory()) {
                // list the wars from this nar
                final File[] narDependencyDirs = narDependencies.listFiles(WAR_FILTER);
                if (narDependencyDirs == null) {
                    throw new IllegalStateException(String.format("Unable to access working directory for NAR dependencies in: %s", narDependencies.getAbsolutePath()));
                }

                // add each war
                for (final File war : narDependencyDirs) {
                    wars.put(war, bundle);
                }
            }
        });

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
     * @param warFile      war
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

    private WebAppContext loadWar(final File warFile, final String contextPath, final ClassLoader parentClassLoader) {
        final WebAppContext webappContext = new WebAppContext(warFile.getPath(), contextPath);
        webappContext.setContextPath(contextPath);
        webappContext.setDisplayName(contextPath);

        // instruction jetty to examine these jars for tlds, web-fragments, etc
        webappContext.setAttribute(CONTAINER_INCLUDE_PATTERN_KEY, CONTAINER_INCLUDE_PATTERN_VALUE);

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

        // add a filter to set the X-Frame-Options filter
        webappContext.addFilter(new FilterHolder(FRAME_OPTIONS_FILTER), "/*", EnumSet.allOf(DispatcherType.class));

        // add a filter to set the Content Security Policy frame-ancestors directive
        FilterHolder cspFilter = new FilterHolder(new ContentSecurityPolicyFilter());
        cspFilter.setName(ContentSecurityPolicyFilter.class.getSimpleName());
        webappContext.addFilter(cspFilter, "/*", EnumSet.allOf(DispatcherType.class));

        try {
            // configure the class loader - webappClassLoader -> jetty nar -> web app's nar -> ...
            webappContext.setClassLoader(new WebAppClassLoader(parentClassLoader, webappContext));
        } catch (final IOException ioe) {
            startUpFailure(ioe);
        }

        logger.info("Loading WAR: " + warFile.getAbsolutePath() + " with context path set to " + contextPath);
        return webappContext;
    }

    private void addDocsServlets(WebAppContext docsContext) {
        try {
            // Load the nifi/docs directory
            final File docsDir = getDocsDir("docs");

            // load the component documentation working directory
            final File componentDocsDirPath = props.getComponentDocumentationWorkingDirectory();
            final File workingDocsDirectory = getWorkingDocsDirectory(componentDocsDirPath);

            // Load the API docs
            final File webApiDocsDir = getWebApiDocsDir();

            // Create the servlet which will serve the static resources
            ServletHolder defaultHolder = new ServletHolder("default", DefaultServlet.class);
            defaultHolder.setInitParameter("dirAllowed", "false");

            ServletHolder docs = new ServletHolder("docs", DefaultServlet.class);
            docs.setInitParameter("resourceBase", docsDir.getPath());

            ServletHolder components = new ServletHolder("components", DefaultServlet.class);
            components.setInitParameter("resourceBase", workingDocsDirectory.getPath());

            ServletHolder restApi = new ServletHolder("rest-api", DefaultServlet.class);
            restApi.setInitParameter("resourceBase", webApiDocsDir.getPath());

            docsContext.addServlet(docs, "/html/*");
            docsContext.addServlet(components, "/components/*");
            docsContext.addServlet(restApi, "/rest-api/*");

            docsContext.addServlet(defaultHolder, "/");

            logger.info("Loading documents web app with context path set to " + docsContext.getContextPath());

        } catch (Exception ex) {
            logger.error("Unhandled Exception in createDocsWebApp: " + ex.getMessage());
            startUpFailure(ex);
        }
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

    private File getWorkingDocsDirectory(final File componentDocsDirPath) {
        File workingDocsDirectory = null;
        try {
            workingDocsDirectory = componentDocsDirPath.toPath().toRealPath().getParent().toFile();
        } catch (IOException ex) {
            logger.error("Failed to load :" + componentDocsDirPath.getAbsolutePath());
            startUpFailure(ex);
        }
        return workingDocsDirectory;
    }

    private File getWebApiDocsDir() {
        // load the rest documentation
        final File webApiDocsDir = new File(webApiContext.getTempDirectory(), "webapp/docs");
        if (!webApiDocsDir.exists()) {
            final boolean made = webApiDocsDir.mkdirs();
            if (!made) {
                logger.error("Failed to create " + webApiDocsDir.getAbsolutePath());
                startUpFailure(new IOException(webApiDocsDir.getAbsolutePath() + " could not be created"));
            }
        }
        return webApiDocsDir;
    }

    private void configureConnectors(final Server server) throws ServerConfigurationException {
        // create the http configuration
        final HttpConfiguration httpConfiguration = new HttpConfiguration();
        final int headerSize = DataUnit.parseDataSize(props.getWebMaxHeaderSize(), DataUnit.B).intValue();
        httpConfiguration.setRequestHeaderSize(headerSize);
        httpConfiguration.setResponseHeaderSize(headerSize);

        // Check if both HTTP and HTTPS connectors are configured and fail if both are configured
        if (bothHttpAndHttpsConnectorsConfigured(props)) {
            logger.error("NiFi only supports one mode of HTTP or HTTPS operation, not both simultaneously. " +
                    "Check the nifi.properties file and ensure that either the HTTP hostname and port or the HTTPS hostname and port are empty");
            startUpFailure(new IllegalStateException("Only one of the HTTP and HTTPS connectors can be configured at one time"));
        }

        if (props.getSslPort() != null) {
            configureHttpsConnector(server, httpConfiguration);
        } else if (props.getPort() != null) {
            configureHttpConnector(server, httpConfiguration);
        } else {
            logger.error("Neither the HTTP nor HTTPS connector was configured in nifi.properties");
            startUpFailure(new IllegalStateException("Must configure HTTP or HTTPS connector"));
        }
    }

    /**
     * Configures an HTTPS connector and adds it to the server.
     *
     * @param server            the Jetty server instance
     * @param httpConfiguration the configuration object for the HTTPS protocol settings
     */
    private void configureHttpsConnector(Server server, HttpConfiguration httpConfiguration) {
        String hostname = props.getProperty(NiFiProperties.WEB_HTTPS_HOST);
        final Integer port = props.getSslPort();
        String connectorLabel = "HTTPS";
        final Map<String, String> httpsNetworkInterfaces = props.getHttpsNetworkInterfaces();
        ServerConnectorCreator<Server, HttpConfiguration, ServerConnector> scc = (s, c) -> createUnconfiguredSslServerConnector(s, c, port);

        configureGenericConnector(server, httpConfiguration, hostname, port, connectorLabel, httpsNetworkInterfaces, scc);
    }

    /**
     * Configures an HTTP connector and adds it to the server.
     *
     * @param server            the Jetty server instance
     * @param httpConfiguration the configuration object for the HTTP protocol settings
     */
    private void configureHttpConnector(Server server, HttpConfiguration httpConfiguration) {
        String hostname = props.getProperty(NiFiProperties.WEB_HTTP_HOST);
        final Integer port = props.getPort();
        String connectorLabel = "HTTP";
        final Map<String, String> httpNetworkInterfaces = props.getHttpNetworkInterfaces();
        ServerConnectorCreator<Server, HttpConfiguration, ServerConnector> scc = (s, c) -> new ServerConnector(s, new HttpConnectionFactory(c));

        configureGenericConnector(server, httpConfiguration, hostname, port, connectorLabel, httpNetworkInterfaces, scc);
    }

    /**
     * Configures an HTTP(S) connector for the server given the provided parameters. The functionality between HTTP and HTTPS connectors is largely similar.
     * Here the common behavior has been extracted into a shared method and the respective calling methods obtain the right values and a lambda function for the differing behavior.
     *
     * @param server                 the Jetty server instance
     * @param configuration          the HTTP/HTTPS configuration instance
     * @param hostname               the hostname from the nifi.properties file
     * @param port                   the port to expose
     * @param connectorLabel         used for log output (e.g. "HTTP" or "HTTPS")
     * @param networkInterfaces      the map of network interfaces from nifi.properties
     * @param serverConnectorCreator a function which accepts a {@code Server} and {@code HttpConnection} instance and returns a {@code ServerConnector}
     */
    private void configureGenericConnector(Server server, HttpConfiguration configuration, String hostname, Integer port, String connectorLabel, Map<String, String> networkInterfaces,
                                           ServerConnectorCreator<Server, HttpConfiguration, ServerConnector> serverConnectorCreator) {
        if (port < 0 || (int) Math.pow(2, 16) <= port) {
            throw new ServerConfigurationException("Invalid " + connectorLabel + " port: " + port);
        }

        logger.info("Configuring Jetty for " + connectorLabel + " on port: " + port);

        final List<Connector> serverConnectors = Lists.newArrayList();

        // Calculate Idle Timeout as twice the auto-refresh interval. This ensures that even with some variance in timing,
        // we are able to avoid closing connections from users' browsers most of the time. This can make a significant difference
        // in HTTPS connections, as each HTTPS connection that is established must perform the SSL handshake.
        final String autoRefreshInterval = props.getAutoRefreshInterval();
        final long autoRefreshMillis = autoRefreshInterval == null ? 30000L : FormatUtils.getTimeDuration(autoRefreshInterval, TimeUnit.MILLISECONDS);
        final long idleTimeout = autoRefreshMillis * 2;

        // If the interfaces collection is empty or each element is empty
        if (networkInterfaces.isEmpty() || networkInterfaces.values().stream().filter(value -> !Strings.isNullOrEmpty(value)).collect(Collectors.toList()).isEmpty()) {
            final ServerConnector serverConnector = serverConnectorCreator.create(server, configuration);

            // Set host and port
            if (StringUtils.isNotBlank(hostname)) {
                serverConnector.setHost(hostname);
            }
            serverConnector.setPort(port);
            serverConnector.setIdleTimeout(idleTimeout);
            serverConnectors.add(serverConnector);
        } else {
            // Add connectors for all IPs from network interfaces
            serverConnectors.addAll(Lists.newArrayList(networkInterfaces.values().stream().map(ifaceName -> {
                NetworkInterface iface = null;
                try {
                    iface = NetworkInterface.getByName(ifaceName);
                } catch (SocketException e) {
                    logger.error("Unable to get network interface by name {}", ifaceName, e);
                }
                if (iface == null) {
                    logger.warn("Unable to find network interface named {}", ifaceName);
                }
                return iface;
            }).filter(Objects::nonNull).flatMap(iface -> Collections.list(iface.getInetAddresses()).stream())
                    .map(inetAddress -> {
                        final ServerConnector serverConnector = serverConnectorCreator.create(server, configuration);

                        // Set host and port
                        serverConnector.setHost(inetAddress.getHostAddress());
                        serverConnector.setPort(port);
                        serverConnector.setIdleTimeout(idleTimeout);

                        return serverConnector;
                    }).collect(Collectors.toList())));
        }
        // Add all connectors
        serverConnectors.forEach(server::addConnector);
    }

    /**
     * Returns true if there are configured properties for both HTTP and HTTPS connectors (specifically port because the hostname can be left blank in the HTTP connector).
     * Prints a warning log message with the relevant properties.
     *
     * @param props the NiFiProperties
     * @return true if both ports are present
     */
    static boolean bothHttpAndHttpsConnectorsConfigured(NiFiProperties props) {
        Integer httpPort = props.getPort();
        String httpHostname = props.getProperty(NiFiProperties.WEB_HTTP_HOST);

        Integer httpsPort = props.getSslPort();
        String httpsHostname = props.getProperty(NiFiProperties.WEB_HTTPS_HOST);

        if (httpPort != null && httpsPort != null) {
            logger.warn("Both the HTTP and HTTPS connectors are configured in nifi.properties. Only one of these connectors should be configured. See the NiFi Admin Guide for more details");
            logger.warn("HTTP connector:   http://" + httpHostname + ":" + httpPort);
            logger.warn("HTTPS connector: https://" + httpsHostname + ":" + httpsPort);
            return true;
        }

        return false;
    }

    private ServerConnector createUnconfiguredSslServerConnector(Server server, HttpConfiguration httpConfiguration, int port) {
        // add some secure config
        final HttpConfiguration httpsConfiguration = new HttpConfiguration(httpConfiguration);
        httpsConfiguration.setSecureScheme("https");
        httpsConfiguration.setSecurePort(port);
        httpsConfiguration.addCustomizer(new SecureRequestCustomizer());

        // build the connector
        return new ServerConnector(server,
                new SslConnectionFactory(createSslContextFactory(), "http/1.1"),
                new HttpConnectionFactory(httpsConfiguration));
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
        String keyStoreType = props.getProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE);
        if (StringUtils.isNotBlank(keyStoreType)) {
            contextFactory.setKeyStoreType(keyStoreType);
            String keyStoreProvider = KeyStoreUtils.getKeyStoreProvider(keyStoreType);
            if (StringUtils.isNoneEmpty(keyStoreProvider)) {
                contextFactory.setKeyStoreProvider(keyStoreProvider);
            }
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
        String trustStoreType = props.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE);
        if (StringUtils.isNotBlank(trustStoreType)) {
            contextFactory.setTrustStoreType(trustStoreType);
            String trustStoreProvider = KeyStoreUtils.getKeyStoreProvider(trustStoreType);
            if (StringUtils.isNoneEmpty(trustStoreProvider)) {
                contextFactory.setTrustStoreProvider(trustStoreProvider);
            }
        }
        if (StringUtils.isNotBlank(props.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD))) {
            contextFactory.setTrustStorePassword(props.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD));
        }
    }

    @Override
    public void start() {
        try {
            // Create a standard extension manager and discover extensions
            final ExtensionDiscoveringManager extensionManager = new StandardExtensionDiscoveringManager();
            extensionManager.discoverExtensions(systemBundle, bundles);
            extensionManager.logClassLoaderMapping();

            // Set the extension manager into the holder which makes it available to the Spring context via a factory bean
            ExtensionManagerHolder.init(extensionManager);

            // Generate docs for extensions
            DocGenerator.generate(props, extensionManager, extensionMapping);

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
                final NiFiWebConfigurationContext configurationContext = webApplicationContext.getBean("nifiWebConfigurationContext", NiFiWebConfigurationContext.class);
                final FilterHolder securityFilter = webApiContext.getServletHandler().getFilter("springSecurityFilterChain");

                // component ui extensions
                performInjectionForComponentUis(componentUiExtensionWebContexts, configurationContext, securityFilter);

                // content viewer extensions
                performInjectionForContentViewerUis(contentViewerWebContexts, securityFilter);

                // content viewer controller
                if (webContentViewerContext != null) {
                    final ContentAccess contentAccess = webApplicationContext.getBean("contentAccess", ContentAccess.class);

                    // add the content access
                    final ServletContext webContentViewerServletContext = webContentViewerContext.getServletHandler().getServletContext();
                    webContentViewerServletContext.setAttribute("nifi-content-access", contentAccess);

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
                    logger.error("Unable to load flow due to: " + e, e);
                    throw new Exception("Unable to load flow due to: " + e); // cannot wrap the exception as they are not defined in a classloader accessible to the caller
                }
            }

            final NarLoader narLoader = new StandardNarLoader(
                    props.getExtensionsWorkingDirectory(),
                    props.getComponentDocumentationWorkingDirectory(),
                    NarClassLoadersHolder.getInstance(),
                    extensionManager,
                    extensionMapping,
                    this);

            narAutoLoader = new NarAutoLoader(props.getNarAutoLoadDirectory(), narLoader);
            narAutoLoader.start();

            // dump the application url after confirming everything started successfully
            dumpUrls();
        } catch (Exception ex) {
            startUpFailure(ex);
        }
    }

    private void performInjectionForComponentUis(final Collection<WebAppContext> componentUiExtensionWebContexts,
                                                 final NiFiWebConfigurationContext configurationContext, final FilterHolder securityFilter) {
        if (CollectionUtils.isNotEmpty(componentUiExtensionWebContexts)) {
            for (final WebAppContext customUiContext : componentUiExtensionWebContexts) {
                // set the NiFi context in each custom ui servlet context
                final ServletContext customUiServletContext = customUiContext.getServletHandler().getServletContext();
                customUiServletContext.setAttribute("nifi-web-configuration-context", configurationContext);

                // add the security filter to any ui extensions wars
                if (securityFilter != null) {
                    customUiContext.addFilter(securityFilter, "/*", EnumSet.allOf(DispatcherType.class));
                }
            }
        }
    }

    private void performInjectionForContentViewerUis(final Collection<WebAppContext> contentViewerWebContexts,
                                                     final FilterHolder securityFilter) {
        if (CollectionUtils.isNotEmpty(contentViewerWebContexts)) {
            for (final WebAppContext contentViewerContext : contentViewerWebContexts) {
                // add the security filter to any content viewer  wars
                if (securityFilter != null) {
                    contentViewerContext.addFilter(securityFilter, "/*", EnumSet.allOf(DispatcherType.class));
                }
            }
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
    public void setBundles(Bundle systemBundle, Set<Bundle> bundles) {
        this.systemBundle = systemBundle;
        this.bundles = bundles;
    }

    @Override
    public void stop() {
        try {
            server.stop();
        } catch (Exception ex) {
            logger.warn("Failed to stop web server", ex);
        }

        try {
            if (narAutoLoader != null) {
                narAutoLoader.stop();
            }
        } catch (Exception e) {
            logger.warn("Failed to stop NAR auto-loader", e);
        }
    }

    private static final Filter FRAME_OPTIONS_FILTER = new Filter() {
        private static final String FRAME_OPTIONS = "X-Frame-Options";
        private static final String SAME_ORIGIN = "SAMEORIGIN";

        @Override
        public void doFilter(final ServletRequest req, final ServletResponse resp, final FilterChain filterChain)
                throws IOException, ServletException {

            // set frame options accordingly
            final HttpServletResponse response = (HttpServletResponse) resp;
            response.setHeader(FRAME_OPTIONS, SAME_ORIGIN);

            filterChain.doFilter(req, resp);
        }

        @Override
        public void init(final FilterConfig config) {
        }

        @Override
        public void destroy() {
        }
    };

    /**
     * Holds the result of loading WARs for custom UIs.
     */
    private static class ExtensionUiInfo {

        private final Collection<WebAppContext> webAppContexts;
        private final Map<String, String> mimeMappings;
        private final Collection<WebAppContext> componentUiExtensionWebContexts;
        private final Collection<WebAppContext> contentViewerWebContexts;
        private final Map<String, List<UiExtension>> componentUiExtensionsByType;

        public ExtensionUiInfo(final Collection<WebAppContext> webAppContexts,
                               final Map<String, String> mimeMappings,
                               final Collection<WebAppContext> componentUiExtensionWebContexts,
                               final Collection<WebAppContext> contentViewerWebContexts,
                               final Map<String, List<UiExtension>> componentUiExtensionsByType) {
            this.webAppContexts = webAppContexts;
            this.mimeMappings = mimeMappings;
            this.componentUiExtensionWebContexts = componentUiExtensionWebContexts;
            this.contentViewerWebContexts = contentViewerWebContexts;
            this.componentUiExtensionsByType = componentUiExtensionsByType;
        }

        public Collection<WebAppContext> getWebAppContexts() {
            return webAppContexts;
        }

        public Map<String, String> getMimeMappings() {
            return mimeMappings;
        }

        public Collection<WebAppContext> getComponentUiExtensionWebContexts() {
            return componentUiExtensionWebContexts;
        }

        public Collection<WebAppContext> getContentViewerWebContexts() {
            return contentViewerWebContexts;
        }

        public Map<String, List<UiExtension>> getComponentUiExtensionsByType() {
            return componentUiExtensionsByType;
        }
    }
}

@FunctionalInterface
interface ServerConnectorCreator<Server, HttpConfiguration, ServerConnector> {
    ServerConnector create(Server server, HttpConfiguration httpConfiguration);
}
