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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import jakarta.servlet.DispatcherType;
import jakarta.servlet.ServletContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.NiFiServer;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.cluster.ClusterDetailsFactory;
import org.apache.nifi.controller.DecommissionTask;
import org.apache.nifi.controller.UninheritableFlowException;
import org.apache.nifi.controller.serialization.FlowSerializationException;
import org.apache.nifi.controller.serialization.FlowSynchronizationException;
import org.apache.nifi.controller.status.history.StatusHistoryDumpFactory;
import org.apache.nifi.diagnostics.DiagnosticsDump;
import org.apache.nifi.diagnostics.DiagnosticsDumpElement;
import org.apache.nifi.diagnostics.DiagnosticsFactory;
import org.apache.nifi.diagnostics.ThreadDumpTask;
import org.apache.nifi.flow.resource.ExternalResourceDescriptor;
import org.apache.nifi.flow.resource.ExternalResourceProvider;
import org.apache.nifi.flow.resource.ExternalResourceProviderInitializationContext;
import org.apache.nifi.flow.resource.ExternalResourceProviderService;
import org.apache.nifi.flow.resource.ExternalResourceProviderServiceBuilder;
import org.apache.nifi.flow.resource.PropertyBasedExternalResourceProviderInitializationContext;
import org.apache.nifi.framework.ssl.FrameworkSslContextProvider;
import org.apache.nifi.lifecycle.LifeCycleStartException;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.ExtensionManagerHolder;
import org.apache.nifi.nar.ExtensionMapping;
import org.apache.nifi.nar.ExtensionUiLoader;
import org.apache.nifi.nar.NarAutoLoader;
import org.apache.nifi.nar.NarClassLoadersHolder;
import org.apache.nifi.nar.NarLoader;
import org.apache.nifi.nar.NarLoaderHolder;
import org.apache.nifi.nar.NarThreadContextClassLoader;
import org.apache.nifi.nar.NarUnpackMode;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.StandardNarLoader;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.ui.extension.UiExtension;
import org.apache.nifi.ui.extension.UiExtensionMapping;
import org.apache.nifi.ui.extension.contentviewer.ContentViewer;
import org.apache.nifi.ui.extension.contentviewer.SupportedMimeTypes;
import org.apache.nifi.util.FileUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.ContentAccess;
import org.apache.nifi.web.NiFiWebConfigurationContext;
import org.apache.nifi.web.UiExtensionType;
import org.apache.nifi.web.server.filter.FilterParameter;
import org.apache.nifi.web.server.filter.LogoutCompleteRedirectFilter;
import org.apache.nifi.web.server.filter.RequestFilterProvider;
import org.apache.nifi.web.server.filter.RestApiRequestFilterProvider;
import org.apache.nifi.web.server.filter.StandardRequestFilterProvider;
import org.eclipse.jetty.deploy.App;
import org.eclipse.jetty.deploy.AppProvider;
import org.eclipse.jetty.deploy.DeploymentManager;
import org.eclipse.jetty.ee10.servlet.FilterMapping;
import org.eclipse.jetty.ee10.servlet.ResourceServlet;
import org.eclipse.jetty.ee10.servlet.ServletHandler;
import org.eclipse.jetty.ee10.webapp.MetaInfConfiguration;
import org.eclipse.jetty.rewrite.handler.RedirectPatternRule;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.ee10.servlet.ErrorPageErrorHandler;
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.ee10.webapp.WebAppClassLoader;
import org.eclipse.jetty.ee10.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import javax.net.ssl.SSLContext;

/**
 * Encapsulates the Jetty instance.
 */
public class JettyServer implements NiFiServer, ExtensionUiLoader {

    private static final Logger logger = LoggerFactory.getLogger(JettyServer.class);

    private static final String ALLOWED_CONTEXT_PATHS_PARAMETER = "allowedContextPaths";
    private static final String CONTAINER_JAR_PATTERN = ".*/jetty-jakarta-servlet-api-[^/]*\\.jar$|.*jakarta.servlet.jsp.jstl-[^/]*\\.jar";

    private static final String CONTEXT_PATH_ALL = "/*";
    private static final String CONTEXT_PATH_NIFI = "/nifi";
    private static final String CONTEXT_PATH_NIFI_API = "/nifi-api";
    private static final Set<String> REQUIRED_CONTEXT_PATHS = Set.of(
            CONTEXT_PATH_NIFI,
            CONTEXT_PATH_NIFI_API
    );

    private static final RequestFilterProvider REQUEST_FILTER_PROVIDER = new StandardRequestFilterProvider();
    private static final RequestFilterProvider REST_API_REQUEST_FILTER_PROVIDER = new RestApiRequestFilterProvider();
    private static final String NAR_PROVIDER_PREFIX = "nifi.nar.library.provider.";
    private static final String NAR_PROVIDER_POLL_INTERVAL_PROPERTY = "nifi.nar.library.poll.interval";
    private static final String NAR_PROVIDER_CONFLICT_RESOLUTION = "nifi.nar.library.conflict.resolution";
    private static final String NAR_PROVIDER_RESTRAIN_PROPERTY = "nifi.nar.library.restrain.startup";
    private static final String NAR_PROVIDER_IMPLEMENTATION_PROPERTY = "implementation";
    private static final String DEFAULT_NAR_PROVIDER_POLL_INTERVAL = "5 min";
    private static final String DEFAULT_NAR_PROVIDER_CONFLICT_RESOLUTION = "IGNORE";

    private static final String NAR_DEPENDENCIES_PATH = "NAR-INF/bundled-dependencies";
    private static final String WAR_EXTENSION = ".war";
    private static final int WEB_APP_MAX_FORM_CONTENT_SIZE = 600000;

    private static final String APPLICATION_PATH = "/nifi";
    private static final String HTTPS_SCHEME = "https";
    private static final String HTTP_SCHEME = "http";
    private static final String HOST_UNSPECIFIED = "0.0.0.0";

    private static final String SPRING_SECURITY_FILTER_CHAIN = "springSecurityFilterChain";

    private static final Duration EXTENSION_UI_POLL_INTERVAL = Duration.ofSeconds(5);

    private final DeploymentManager deploymentManager = new DeploymentManager();

    private Server server;
    private NiFiProperties props;
    private SSLContext sslContext;

    private Bundle systemBundle;
    private Set<Bundle> bundles;
    private ExtensionMapping extensionMapping;
    private NarAutoLoader narAutoLoader;
    private ExternalResourceProviderService narProviderService;
    private DiagnosticsFactory diagnosticsFactory;
    private DecommissionTask decommissionTask;
    private StatusHistoryDumpFactory statusHistoryDumpFactory;
    private ClusterDetailsFactory clusterDetailsFactory;

    private WebAppContext webApiContext;

    // content viewer and mime type specific extensions
    private Collection<WebAppContext> contentViewerWebContexts;
    private Collection<ContentViewer> contentViewers;

    // component (processor, controller service, reporting task) ui extensions
    private UiExtensionMapping componentUiExtensions;
    private Collection<WebAppContext> componentUiExtensionWebContexts;

    private final Map<BundleCoordinate, List<App>> appsByBundleCoordinate = new ConcurrentHashMap<>();

    private final BlockingQueue<Bundle> extensionUisToLoad = new LinkedBlockingQueue<>();
    private final ExtensionUiLoadTask extensionUiLoadTask = new ExtensionUiLoadTask(extensionUisToLoad, this::processExtensionUiBundle);

    /**
     * Default no-arg constructor for ServiceLoader
     */
    public JettyServer() {
    }

    public void init() {
        clearWorkingDirectory();

        try {
            final FrameworkSslContextProvider sslContextProvider = new FrameworkSslContextProvider(props);
            sslContext = sslContextProvider.loadSslContext().orElse(null);

            final ServerProvider serverProvider = new StandardServerProvider(sslContext);
            server = serverProvider.getServer(props);

            final Handler serverHandler = server.getHandler();
            if (serverHandler instanceof Handler.Collection serverHandlerCollection) {
                final ContextHandlerCollection contextHandlerCollection = new ContextHandlerCollection();
                final Handler warHandlers = loadInitialWars(bundles);
                contextHandlerCollection.addHandler(warHandlers);
                deploymentManager.setContexts(contextHandlerCollection);
                server.addBean(deploymentManager);

                serverHandlerCollection.addHandler(contextHandlerCollection);
            } else {
                throw new IllegalStateException("Server Handler not Handler.Collection: Server Provider configuration failed");
            }
        } catch (final Throwable e) {
            startUpFailure(e);
        }
    }

    private void clearWorkingDirectory() {
        // Clear the working directory to ensure that Jetty loads the latest WAR application files
        final File webWorkingDir = props.getWebWorkingDirectory();
        try {
            FileUtils.deleteFilesInDirectory(webWorkingDir, null, logger, true, true);
        } catch (final IOException e) {
            logger.warn("Clear Working Directory failed [{}]", webWorkingDir, e);
        }
        FileUtils.deleteFile(webWorkingDir, logger, 3);
    }

    private Handler loadInitialWars(final Set<Bundle> bundles) {
        final Map<File, Bundle> warToBundleLookup = findWars(bundles);

        // locate each war being deployed
        File webUiWar = null;
        File webApiWar = null;
        Map<File, Bundle> otherWars = new HashMap<>();
        for (Map.Entry<File, Bundle> warBundleEntry : warToBundleLookup.entrySet()) {
            final File war = warBundleEntry.getKey();
            final Bundle warBundle = warBundleEntry.getValue();

            if (war.getName().toLowerCase().startsWith("nifi-web-api")) {
                webApiWar = war;
            } else if (war.getName().toLowerCase().startsWith("nifi-ui")) {
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
        }

        // handlers for each war and init params for the web api
        final ExtensionUiInfo extensionUiInfo = loadWars(otherWars);
        componentUiExtensionWebContexts = new ArrayList<>(extensionUiInfo.componentUiExtensionWebContexts());
        contentViewerWebContexts = new ArrayList<>(extensionUiInfo.contentViewerWebContexts());
        contentViewers = new HashSet<>(extensionUiInfo.contentViewers());
        componentUiExtensions = new UiExtensionMapping(extensionUiInfo.componentUiExtensionsByType());

        final ContextHandlerCollection webAppContextHandlers = new ContextHandlerCollection();
        final Collection<WebAppContext> extensionUiContexts = extensionUiInfo.webAppContexts();
        extensionUiContexts.forEach(webAppContextHandlers::addHandler);

        final ClassLoader frameworkClassLoader = getClass().getClassLoader();

        // load the web ui app
        final WebAppContext webUiContext = loadWar(webUiWar, CONTEXT_PATH_NIFI, frameworkClassLoader);

        // add a rewrite error handler for the ui to handle 404
        final RewriteHandler uiErrorHandler = new RewriteHandler();
        uiErrorHandler.setServer(server);
        final RedirectPatternRule redirectToUi = new RedirectPatternRule("/*", "/nifi/#/404");
        uiErrorHandler.addRule(redirectToUi);
        webUiContext.setErrorHandler(uiErrorHandler);

        // load the web api app
        webApiContext = loadWar(webApiWar, CONTEXT_PATH_NIFI_API, frameworkClassLoader);
        webAppContextHandlers.addHandler(webApiContext);

        // add the servlets which serve the HTML documentation within the documentation web app
        addDocsServlets(webApiContext);
        webAppContextHandlers.addHandler(webUiContext);

        // deploy the web apps
        return webAppContextHandlers;
    }

    @Override
    public synchronized void loadExtensionUis(final Collection<Bundle> bundles) {
        extensionUisToLoad.addAll(bundles);
    }

    private void processExtensionUiBundle(final Bundle bundle) {
        // Find and load any WARs contained within the set of bundles...
        final Map<File, Bundle> warToBundleLookup = findWars(Set.of(bundle));
        final ExtensionUiInfo extensionUiInfo = loadWars(warToBundleLookup);
        final Map<BundleCoordinate, List<WebAppContext>> webappContextsByBundleCoordinate = extensionUiInfo.webAppContextsByBundleCoordinate();

        final Collection<WebAppContext> webAppContexts = extensionUiInfo.webAppContexts();
        if (webAppContexts.isEmpty()) {
            logger.debug("Extension User Interface Web Applications not found");
            return;
        }

        for (final Map.Entry<BundleCoordinate, List<WebAppContext>> entry : webappContextsByBundleCoordinate.entrySet()) {
            for (final WebAppContext webAppContext : entry.getValue()) {
                final Path warPath = Paths.get(webAppContext.getWar());
                final App extensionUiApp = new ExtensionUiApp(deploymentManager, null, warPath, webAppContext);
                deploymentManager.addApp(extensionUiApp);

                final List<App> bundleApps = appsByBundleCoordinate.computeIfAbsent(entry.getKey(), (k) -> new ArrayList<>());
                bundleApps.add(extensionUiApp);
            }
        }

        final Collection<WebAppContext> componentUiExtensionWebContexts = extensionUiInfo.componentUiExtensionWebContexts();
        final Collection<WebAppContext> contentViewerWebContexts = extensionUiInfo.contentViewerWebContexts();

        // Inject the configuration context and security filter into contexts that need it
        final ServletContext webApiServletContext = webApiContext.getServletHandler().getServletContext();
        final WebApplicationContext webApplicationContext = WebApplicationContextUtils.getRequiredWebApplicationContext(webApiServletContext);
        final NiFiWebConfigurationContext configurationContext = webApplicationContext.getBean("nifiWebConfigurationContext", NiFiWebConfigurationContext.class);
        final FilterHolder securityFilter = webApiContext.getServletHandler().getFilter(SPRING_SECURITY_FILTER_CHAIN);

        performInjectionForComponentUis(componentUiExtensionWebContexts, configurationContext, securityFilter);
        performInjectionForContentViewerUis(contentViewerWebContexts, webApplicationContext, securityFilter);

        // Merge results of current loading into previously loaded results...
        this.componentUiExtensionWebContexts.addAll(componentUiExtensionWebContexts);
        this.contentViewerWebContexts.addAll(contentViewerWebContexts);
        this.contentViewers.addAll(extensionUiInfo.contentViewers());
        this.componentUiExtensions.addUiExtensions(extensionUiInfo.componentUiExtensionsByType());

        for (final WebAppContext webAppContext : webAppContexts) {
            final Throwable unavailableException = webAppContext.getUnavailableException();
            if (unavailableException == null) {
                logger.debug("Web Application [{}] loaded", webAppContext);
            } else {
                logger.error("Web Application [{}] unavailable after initialization", webAppContext, unavailableException);
            }
        }
    }

    @Override
    public synchronized void unloadExtensionUis(final Collection<Bundle> bundles) {
        bundles.forEach(this::unloadExtensionUis);
    }

    private void unloadExtensionUis(final Bundle bundle) {
        final BundleCoordinate bundleCoordinate = bundle.getBundleDetails().getCoordinate();
        final List<App> bundleApps = appsByBundleCoordinate.remove(bundleCoordinate);
        if (bundleApps == null) {
            logger.info("No Extension UI WARs exist from bundle [{}]", bundleCoordinate);
            return;
        }

        logger.info("Unloading {} Extension UI WARs from bundle [{}]", bundleApps.size(), bundleCoordinate);
        bundleApps.forEach(app -> unloadApp(bundleCoordinate, app));
        componentUiExtensions.removeUiExtensions(bundleCoordinate.getGroup(), bundleCoordinate.getId(), bundleCoordinate.getVersion());
        contentViewers.removeAll(contentViewers.stream().filter((contentViewer -> bundle.equals(contentViewer.getBundle()))).toList());
    }

    private void unloadApp(final BundleCoordinate bundleCoordinate, final App app) {
        logger.info("Unloading Extension UI WAR with context path [{}] from bundle [{}]", app.getContextPath(), bundleCoordinate);
        try {
            // Need to remove the filter mapping for the security filter chain before calling removeApp,
            // otherwise it will impact the security filter chain which is shared across all contexts
            final WebAppContext webAppContext = (WebAppContext) app.getContextHandler();
            final ServletHandler webAppServletHandler = webAppContext.getServletHandler();

            final FilterMapping[] webAppFilterMappings = webAppServletHandler.getFilterMappings();
            if (webAppFilterMappings != null) {
                Arrays.stream(webAppFilterMappings)
                        .filter(filterMapping -> filterMapping.getFilterName().equals(SPRING_SECURITY_FILTER_CHAIN))
                        .findFirst()
                        .ifPresent(webAppServletHandler::removeFilterMapping);
            }

            final FilterHolder[] webAppFilterHolders = webAppServletHandler.getFilters();
            if (webAppFilterHolders != null) {
                Arrays.stream(webAppFilterHolders)
                        .filter(filterHolder -> filterHolder.getName().equals(SPRING_SECURITY_FILTER_CHAIN))
                        .findFirst()
                        .ifPresent(webAppServletHandler::removeFilterHolder);
            }

            deploymentManager.removeApp(app);
            contentViewerWebContexts.removeIf(context -> context.getContextPath().equals(app.getContextPath()));
            componentUiExtensionWebContexts.removeIf(context -> context.getContextPath().equals(app.getContextPath()));

            final File appWarFile = app.getPath().toFile();
            if (appWarFile.exists()) {
                if (!appWarFile.delete()) {
                    logger.warn("Failed to delete WAR file at [{}]", appWarFile.getAbsolutePath());
                }
            }
        } catch (final Exception e) {
            logger.error("Failed to unload Extension UI WAR with context path [{}] from bundle [{}]", app.getContextPath(), bundleCoordinate);
        }
    }

    private ExtensionUiInfo loadWars(final Map<File, Bundle> warToBundleLookup) {
        // handlers for each war and init params for the web api
        final List<WebAppContext> webAppContexts = new ArrayList<>();
        final Map<String, String> mimeMappings = new HashMap<>();
        final Collection<WebAppContext> componentUiExtensionWebContexts = new ArrayList<>();
        final Collection<WebAppContext> contentViewerWebContexts = new ArrayList<>();
        final Map<String, List<UiExtension>> componentUiExtensionsByType = new HashMap<>();
        final Map<String, ServletContext> contentViewerServletContexts = new HashMap<>();
        final Map<BundleCoordinate, List<WebAppContext>> webAppContextsByBundleCoordinate = new HashMap<>();
        final Collection<ContentViewer> contentViewers = new ArrayList<>();

        // deploy the other wars
        if (!warToBundleLookup.isEmpty()) {
            // ui extension organized by component type
            for (Map.Entry<File, Bundle> warBundleEntry : warToBundleLookup.entrySet()) {
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
                    final ClassLoader narClassLoaderForWar = warBundle.getClassLoader();

                    // create the extension web app context
                    WebAppContext extensionUiContext = loadWar(war, warContextPath, narClassLoaderForWar);

                    // create the ui extensions
                    for (final Map.Entry<UiExtensionType, List<String>> entry : uiExtensionInWar.entrySet()) {
                        final UiExtensionType extensionType = entry.getKey();
                        final List<String> types = entry.getValue();

                        if (UiExtensionType.ContentViewer.equals(extensionType)) {
                            final List<SupportedMimeTypes> supportedMimeTypes = new ArrayList<>();

                            // consider each content type identified
                            for (final String supportedContentTypes : types) {
                                final String[] parts = supportedContentTypes.split("=");
                                if (parts.length == 2) {
                                    final String displayName = parts[0];
                                    final String contentTypes = parts[1];

                                    final String[] contentTypeParts = contentTypes.split(",");
                                    for (final String contentType : contentTypeParts) {
                                        // map the content type to the context path
                                        mimeMappings.put(contentType, warContextPath);
                                    }

                                    supportedMimeTypes.add(new SupportedMimeTypes(displayName, List.of(contentTypeParts)));
                                }
                            }
                            contentViewers.add(new ContentViewer(warContextPath, supportedMimeTypes, warBundle));

                            // this ui extension provides a content viewer
                            contentViewerWebContexts.add(extensionUiContext);

                            // Add mapping from Context Path to Servlet Context for ContentViewerController
                            contentViewerServletContexts.put(warContextPath, extensionUiContext.getServletContext());
                        } else {
                            // consider each component type identified
                            for (final String componentTypeCoordinates : types) {
                                logger.info("Loading UI extension [{}, {}] for {}", extensionType, warContextPath, componentTypeCoordinates);

                                // record the extension definition
                                final UiExtension uiExtension = new UiExtension(extensionType, warContextPath);

                                // create if this is the first extension for this component type
                                final List<UiExtension> componentUiExtensionsForType = componentUiExtensionsByType.computeIfAbsent(componentTypeCoordinates, k -> new ArrayList<>());

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

                    final BundleCoordinate bundleCoordinate = warBundle.getBundleDetails().getCoordinate();
                    final List<WebAppContext> bundleContexts = webAppContextsByBundleCoordinate.computeIfAbsent(bundleCoordinate, (k) -> new ArrayList<>());
                    bundleContexts.add(extensionUiContext);
                }
            }
        }

        return new ExtensionUiInfo(webAppContexts, mimeMappings, contentViewers, componentUiExtensionWebContexts, contentViewerWebContexts,
                componentUiExtensionsByType, contentViewerServletContexts, webAppContextsByBundleCoordinate);
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

    private Map<File, Bundle> findWars(final Set<Bundle> bundles) {
        final Map<File, Bundle> wars = new HashMap<>();

        bundles.forEach(bundle -> {
            final BundleDetails details = bundle.getBundleDetails();
            final Path bundledDependencies = new File(details.getWorkingDirectory(), NAR_DEPENDENCIES_PATH).toPath();
            if (Files.isDirectory(bundledDependencies)) {
                try (Stream<Path> dependencies = Files.list(bundledDependencies)) {
                    dependencies.filter(dependency -> dependency.getFileName().toString().endsWith(WAR_EXTENSION))
                            .map(Path::toFile)
                            .forEach(dependency -> wars.put(dependency, bundle));
                } catch (final IOException e) {
                    logger.warn("Failed to find WAR files in bundled-dependencies [{}]", bundledDependencies, e);
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
                    final List<String> extensions = uiExtensions.computeIfAbsent(uiExtensionType, k -> new ArrayList<>());
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
            readUiExtensions(uiExtensions, UiExtensionType.ParameterProviderConfiguration, jarFile, jarFile.getJarEntry("META-INF/nifi-parameter-provider-configuration"));
            readUiExtensions(uiExtensions, UiExtensionType.FlowRegistryClientConfiguration, jarFile, jarFile.getJarEntry("META-INF/nifi-flow-registry-client-configuration"));
        } catch (IOException ioe) {
            logger.warn("Unable to inspect {} for a UI extensions.", warFile);
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
        webappContext.getInitParams().put(ALLOWED_CONTEXT_PATHS_PARAMETER, props.getAllowedContextPaths());
        webappContext.setMaxFormContentSize(WEB_APP_MAX_FORM_CONTENT_SIZE);
        webappContext.setAttribute(MetaInfConfiguration.CONTAINER_JAR_PATTERN, CONTAINER_JAR_PATTERN);
        webappContext.setErrorHandler(getErrorHandler());
        webappContext.setTempDirectory(getWebAppTempDirectory(warFile));

        final boolean throwUnavailableOnStartupException = REQUIRED_CONTEXT_PATHS.contains(contextPath);
        webappContext.setThrowUnavailableOnStartupException(throwUnavailableOnStartupException);

        final List<FilterHolder> requestFilters = CONTEXT_PATH_NIFI_API.equals(contextPath)
                ? REST_API_REQUEST_FILTER_PROVIDER.getFilters(props)
                : REQUEST_FILTER_PROVIDER.getFilters(props);

        // Add Logout Complete Filter for web user interface integration
        if (CONTEXT_PATH_NIFI.equals(contextPath)) {
            final FilterHolder logoutCompleteFilterHolder = new FilterHolder(LogoutCompleteRedirectFilter.class);
            logoutCompleteFilterHolder.setName(LogoutCompleteRedirectFilter.class.getSimpleName());
            requestFilters.add(logoutCompleteFilterHolder);
        }

        requestFilters.forEach(filter -> {
            final String pathSpecification = filter.getInitParameter(FilterParameter.PATH_SPECIFICATION.name());
            final String filterPathSpecification = pathSpecification == null ? CONTEXT_PATH_ALL : pathSpecification;
            webappContext.addFilter(filter, filterPathSpecification, EnumSet.allOf(DispatcherType.class));
        });

        // configure the class loader - webappClassLoader -> jetty nar -> web app's nar -> ...
        webappContext.setClassLoader(new WebAppClassLoader(parentClassLoader, webappContext));

        logger.info("Loading WAR [{}] Context Path [{}]", warFile.getAbsolutePath(), contextPath);
        return webappContext;
    }

    private File getWebAppTempDirectory(final File warFile) {
        final File tempDirectory = new File(props.getWebWorkingDirectory(), warFile.getName()).getAbsoluteFile();

        if (tempDirectory.exists() && !tempDirectory.isDirectory()) {
            throw new IllegalStateException("Web Application Temporary Directory [%s] is not a directory".formatted(tempDirectory));
        } else if (!tempDirectory.exists()) {
            final boolean created = tempDirectory.mkdirs();
            if (created) {
                logger.debug("Web Application Temporary Directory [{}] created", tempDirectory);
            } else {
                throw new IllegalStateException("Web Application Temporary Directory [%s] directory creation failed".formatted(tempDirectory));
            }
        }

        if (!tempDirectory.canRead()) {
            throw new IllegalStateException("Web Application Temporary Directory [%s] is missing read permission".formatted(tempDirectory));
        }
        if (!tempDirectory.canWrite()) {
            throw new IllegalStateException("Web Application Temporary Directory [%s] is missing write permissions".formatted(tempDirectory));
        }

        return tempDirectory;
    }

    private void addDocsServlets(WebAppContext webAppContext) {
        try {
            final File docsDir = getDocsDir();

            final ServletHolder docs = new ServletHolder("docs", ResourceServlet.class);
            final Path htmlBaseResource = docsDir.toPath().resolve("html");
            docs.setInitParameter("baseResource", htmlBaseResource.toString());
            docs.setInitParameter("dirAllowed", "false");
            webAppContext.addServlet(docs, "/html/*");

            final ServletHolder restApi = new ServletHolder("rest-api", ResourceServlet.class);
            final File webApiDocsDir = getWebApiDocsDir();
            restApi.setInitParameter("baseResource", webApiDocsDir.getPath());
            restApi.setInitParameter("dirAllowed", "false");
            webAppContext.addServlet(restApi, "/rest-api/*");

            logger.info("Loading Docs [{}] Context Path [{}]", docsDir.getAbsolutePath(), webAppContext.getContextPath());
        } catch (Exception ex) {
            logger.error("Unhandled Exception in createDocsWebApp", ex);
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
     * @return A File object to the documentation directory; else startUpFailure called.
     */
    private File getDocsDir() {
        final String docsDirectory = "docs";
        File docsDir;
        try {
            docsDir = Paths.get(docsDirectory).toRealPath().toFile();
        } catch (IOException ex) {
            logger.info("Directory '{}' is missing. Some documentation will be unavailable.", docsDirectory);
            docsDir = new File(docsDirectory).getAbsoluteFile();
            final boolean made = docsDir.mkdirs();
            if (!made) {
                logger.error("Failed to create 'docs' directory!");
                startUpFailure(new IOException(docsDir.getAbsolutePath() + " could not be created"));
            }
        }
        return docsDir;
    }

    private File getWebApiDocsDir() {
        // load the rest documentation
        final File webApiDocsDir = new File(webApiContext.getTempDirectory(), "webapp/docs/rest-api");
        if (!webApiDocsDir.exists()) {
            final boolean made = webApiDocsDir.mkdirs();
            if (!made) {
                logger.error("Failed to create {}", webApiDocsDir.getAbsolutePath());
                startUpFailure(new IOException(webApiDocsDir.getAbsolutePath() + " could not be created"));
            }
        }
        return webApiDocsDir;
    }

    protected List<URI> getApplicationUrls() {
        return Arrays.stream(server.getConnectors())
                .map(connector -> (ServerConnector) connector)
                .map(serverConnector -> {
                    final SslConnectionFactory sslConnectionFactory = serverConnector.getConnectionFactory(SslConnectionFactory.class);
                    final String scheme = sslConnectionFactory == null ? HTTP_SCHEME : HTTPS_SCHEME;
                    final int port = serverConnector.getLocalPort();
                    final String connectorHost = serverConnector.getHost();
                    final String host = StringUtils.defaultIfEmpty(connectorHost, HOST_UNSPECIFIED);
                    try {
                        return new URI(scheme, null, host, port, APPLICATION_PATH, null, null);
                    } catch (URISyntaxException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
    }

    @Override
    public void start() {
        try {
            // Create a standard extension manager and discover extensions
            final StandardExtensionDiscoveringManager extensionManager = new StandardExtensionDiscoveringManager();
            extensionManager.discoverExtensions(systemBundle, bundles);
            extensionManager.logClassLoaderMapping();

            // Set the extension manager into the holder which makes it available to the Spring context via a factory bean
            ExtensionManagerHolder.init(extensionManager);

            // Additionally loaded NARs and collected flow resources must be in place before starting the flows
            narProviderService = new ExternalResourceProviderServiceBuilder("NAR Auto-Loader Provider", extensionManager)
                    .providers(buildExternalResourceProviders(sslContext, extensionManager, descriptor -> descriptor.getLocation().toLowerCase().endsWith(".nar")))
                    .targetDirectory(new File(props.getProperty(NiFiProperties.NAR_LIBRARY_AUTOLOAD_DIRECTORY, NiFiProperties.DEFAULT_NAR_LIBRARY_AUTOLOAD_DIR)))
                    .conflictResolutionStrategy(props.getProperty(NAR_PROVIDER_CONFLICT_RESOLUTION, DEFAULT_NAR_PROVIDER_CONFLICT_RESOLUTION))
                    .pollInterval(props.getProperty(NAR_PROVIDER_POLL_INTERVAL_PROPERTY, DEFAULT_NAR_PROVIDER_POLL_INTERVAL))
                    .restrainingStartup(Boolean.parseBoolean(props.getProperty(NAR_PROVIDER_RESTRAIN_PROPERTY, "true")))
                    .build();
            narProviderService.start();

            // The NarAutoLoader must be started after the provider has started because we don't want the loader to load anything
            // until all provided NARs are available, in case there is a dependency between any of the provided NARs
            final NarUnpackMode unpackMode = props.isUnpackNarsToUberJar() ? NarUnpackMode.UNPACK_TO_UBER_JAR : NarUnpackMode.UNPACK_INDIVIDUAL_JARS;
            final NarLoader narLoader = new StandardNarLoader(
                    props.getExtensionsWorkingDirectory(),
                    NarClassLoadersHolder.getInstance(),
                    extensionManager,
                    extensionMapping,
                    this,
                    unpackMode);

            // Set the NarLoader into the holder which makes it available to the Spring context via a factory bean
            NarLoaderHolder.init(narLoader);

            narAutoLoader = new NarAutoLoader(props, narLoader);
            narAutoLoader.start();

            server.start();

            // ensure the appropriate wars deployed successfully before injecting the NiFi context and security filters
            // this must be done after starting the server (and ensuring there were no start up failures)
            if (webApiContext != null) {
                // give the web api the component ui extensions
                final ServletContext webApiServletContext = webApiContext.getServletHandler().getServletContext();
                webApiServletContext.setAttribute("nifi-ui-extensions", componentUiExtensions);

                // get the application context
                final WebApplicationContext webApplicationContext = WebApplicationContextUtils.getRequiredWebApplicationContext(webApiServletContext);
                final NiFiWebConfigurationContext configurationContext = webApplicationContext.getBean("nifiWebConfigurationContext", NiFiWebConfigurationContext.class);
                final FilterHolder securityFilter = webApiContext.getServletHandler().getFilter(SPRING_SECURITY_FILTER_CHAIN);

                // component ui extensions
                performInjectionForComponentUis(componentUiExtensionWebContexts, configurationContext, securityFilter);

                // content viewer extensions
                performInjectionForContentViewerUis(contentViewerWebContexts, webApplicationContext, securityFilter);
                webApiServletContext.setAttribute("content-viewers", contentViewers);

                diagnosticsFactory = webApplicationContext.getBean("diagnosticsFactory", DiagnosticsFactory.class);
                decommissionTask = webApplicationContext.getBean("decommissionTask", DecommissionTask.class);
                statusHistoryDumpFactory = webApplicationContext.getBean("statusHistoryDumpFactory", StatusHistoryDumpFactory.class);
                clusterDetailsFactory = webApplicationContext.getBean("clusterDetailsFactory", ClusterDetailsFactory.class);
            }

            // Start background task to process bundles that are submitted for loading extension UIs, this needs to be
            // started after Jetty has been started to ensure the Spring WebApplicationContext is available
            Thread.ofVirtual().name("Extension UI Loader").start(extensionUiLoadTask);

            // if this nifi is a node in a cluster, start the flow service and load the flow - the
            // flow service is loaded here for clustered nodes because the loading of the flow will
            // initialize the connection between the node and the coordinator. if the node connects (starts
            // heartbeating, etc), the coordinator may issue web requests before the application (wars) have
            // finished loading. this results in the node being disconnected since its unable to
            // successfully respond to the requests. to resolve this, flow loading was moved to here
            // (after the wars have been successfully deployed) when this nifi instance is a node
            // in a cluster
            if (props.isNode()) {

                FlowService flowService = null;
                try {
                    ApplicationContext ctx = WebApplicationContextUtils.getWebApplicationContext(webApiContext.getServletContext());
                    flowService = Objects.requireNonNull(ctx).getBean("flowService", FlowService.class);

                    // start and load the flow
                    flowService.start();
                    flowService.load(null);
                } catch (BeansException | LifeCycleStartException | IOException | FlowSerializationException | FlowSynchronizationException | UninheritableFlowException e) {
                    // ensure the flow service is terminated
                    if (flowService != null && flowService.isRunning()) {
                        flowService.stop(false);
                    }
                    logger.error("Failed to start Flow Service", e);
                    throw new Exception("Failed to start Flow Service: " + e); // cannot wrap the exception as they are not defined in a classloader accessible to the caller
                }
            }

            final List<URI> applicationUrls = getApplicationUrls();
            if (applicationUrls.isEmpty()) {
                logger.warn("Started Server without connectors");
            } else {
                for (final URI applicationUrl : applicationUrls) {
                    logger.info("Started Server on {}", applicationUrl);
                }
            }
        } catch (Throwable t) {
            startUpFailure(t);
        }
    }

    private Map<String, ExternalResourceProvider> buildExternalResourceProviders(
            final SSLContext sslContext,
            final ExtensionManager extensionManager,
            final Predicate<ExternalResourceDescriptor> filter
    )
        throws ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {

        final Map<String, ExternalResourceProvider> result = new HashMap<>();
        final Set<String> externalSourceNames = props.getDirectSubsequentTokens(NAR_PROVIDER_PREFIX);

        for (final String externalSourceName : externalSourceNames) {
            logger.info("External resource provider '{}' found in configuration", externalSourceName);

            final String providerClass = props.getProperty(NAR_PROVIDER_PREFIX + externalSourceName + "." + NAR_PROVIDER_IMPLEMENTATION_PROPERTY);
            final String providerId = UUID.randomUUID().toString();

            final ExternalResourceProviderInitializationContext context
                    = new PropertyBasedExternalResourceProviderInitializationContext(sslContext, props, NAR_PROVIDER_PREFIX + externalSourceName + ".", filter);
            result.put(providerId, createProviderInstance(extensionManager, providerClass, providerId, context));
        }

        return result;
    }

    /**
     * In case the provider class is not an implementation of {@code ExternalResourceProvider} the method tries to instantiate it as a {@code NarProvider}. {@code NarProvider} instances
     * are wrapped into an adapter in order to envelope the support.
     */
    private ExternalResourceProvider createProviderInstance(final ExtensionManager extensionManager, final String providerClass, final String providerId,
                                                            final ExternalResourceProviderInitializationContext context)
        throws InstantiationException, IllegalAccessException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException {

        ExternalResourceProvider provider;
        try {
            provider = NarThreadContextClassLoader.createInstance(extensionManager, providerClass, ExternalResourceProvider.class, props, providerId);
        } catch (final ClassCastException e) {
            throw new IllegalArgumentException(String.format("Class %s does not implement ExternalResourceProvider", providerClass), e);
        }

        provider.initialize(context);
        return provider;
    }

    @Override
    public DiagnosticsFactory getDiagnosticsFactory() {
        // The diagnosticsFactory is initialized during server startup. If the diagnostics factory happens to be
        // requested before the Server starts, or after the server fails to start, we cannot provide the fully initialized
        // diagnostics factory. But it is still helpful to provide what we can, so we will provide the Thread Dump Factory.
        return diagnosticsFactory == null ? getThreadDumpFactory() : diagnosticsFactory;
    }

    @Override
    public DiagnosticsFactory getThreadDumpFactory() {
        return new ThreadDumpDiagnosticsFactory();
    }

    @Override
    public DecommissionTask getDecommissionTask() {
        return decommissionTask;
    }

    @Override
    public ClusterDetailsFactory getClusterDetailsFactory() {
        return clusterDetailsFactory;
    }

    @Override
    public StatusHistoryDumpFactory getStatusHistoryDumpFactory() {
        return statusHistoryDumpFactory;
    }


    private void performInjectionForComponentUis(final Collection<WebAppContext> componentUiExtensionWebContexts,
                                                 final NiFiWebConfigurationContext configurationContext, final FilterHolder securityFilter) {
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

    private void performInjectionForContentViewerUis(final Collection<WebAppContext> contentViewerWebContexts,
                                                     final WebApplicationContext webApiApplicationContext,
                                                     final FilterHolder securityFilter) {
        for (final WebAppContext contentViewerContext : contentViewerWebContexts) {
            final ContentAccess contentAccess = webApiApplicationContext.getBean("contentAccess", ContentAccess.class);

            // add the content access
            final ServletContext webContentViewerServletContext = contentViewerContext.getServletHandler().getServletContext();
            webContentViewerServletContext.setAttribute("nifi-content-access", contentAccess);

            // add the security filter to any content viewer  wars
            if (securityFilter != null) {
                contentViewerContext.addFilter(securityFilter, "/*", EnumSet.allOf(DispatcherType.class));
            }
        }
    }

    private void startUpFailure(final Throwable t) {
        logger.error("Failed to start Server", t);
        System.exit(1);
    }

    @Override
    public void initialize(final NiFiProperties properties, final Bundle systemBundle, final Set<Bundle> bundles, final ExtensionMapping extensionMapping) {
        this.props = properties;
        this.systemBundle = systemBundle;
        this.bundles = bundles;
        this.extensionMapping = extensionMapping;

        init();
    }

    @Override
    public void stop() {
        try {
            server.stop();
        } catch (final Exception e) {
            logger.warn("Failed to stop Server", e);
        }

        try {
            if (narAutoLoader != null) {
                narAutoLoader.stop();
            }
        } catch (Exception e) {
            logger.warn("Failed to stop NAR auto-loader", e);
        }

        try {
            if (narProviderService != null) {
                narProviderService.stop();
            }
        } catch (Exception e) {
            logger.warn("Failed to stop NAR provider", e);
        }

        extensionUiLoadTask.stop();
    }

    private ErrorPageErrorHandler getErrorHandler() {
        final ErrorPageErrorHandler errorHandler = new ErrorPageErrorHandler();
        errorHandler.setShowServlet(false);
        errorHandler.setShowStacks(false);
        errorHandler.setShowMessageInTitle(false);
        return errorHandler;
    }

    private record ExtensionUiInfo(Collection<WebAppContext> webAppContexts, Map<String, String> mimeMappings,
                                   Collection<ContentViewer> contentViewers, Collection<WebAppContext> componentUiExtensionWebContexts,
                                   Collection<WebAppContext> contentViewerWebContexts,
                                   Map<String, List<UiExtension>> componentUiExtensionsByType,
                                   Map<String, ServletContext> contentViewerServletContexts,
                                   Map<BundleCoordinate, List<WebAppContext>> webAppContextsByBundleCoordinate) {

    }

    private static class ThreadDumpDiagnosticsFactory implements DiagnosticsFactory {
        @Override
        public DiagnosticsDump create(final boolean verbose) {
            return out -> {
                final DiagnosticsDumpElement threadDumpElement = new ThreadDumpTask().captureDump(verbose);
                final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
                for (final String detail : threadDumpElement.getDetails()) {
                    writer.write(detail);
                    writer.write("\n");
                }

                writer.flush();
            };
        }
    }

    /**
     * Extension of Jetty's {@link App} class to allow use of an already created {@link WebAppContext}.
     */
    private static class ExtensionUiApp extends App {

        private final WebAppContext webAppContext;

        public ExtensionUiApp(final DeploymentManager manager, final AppProvider provider, final Path path, final WebAppContext webAppContext) {
            super(manager, provider, path);
            this.webAppContext = webAppContext;
        }

        @Override
        public ContextHandler getContextHandler() {
            return webAppContext;
        }

        @Override
        public String getContextPath() {
            return webAppContext.getContextPath();
        }
    }

    /**
     * Task that asynchronously processes any bundles that were submitted to have extension UIs loaded.
     */
    private static class ExtensionUiLoadTask implements Runnable {

        private final BlockingQueue<Bundle> extensionUiBundlesToLoad;
        private final Consumer<Bundle> extensionUiLoadFunction;

        private volatile boolean stopped = false;

        public ExtensionUiLoadTask(final BlockingQueue<Bundle> extensionUiBundlesToLoad, final Consumer<Bundle> extensionUiLoadFunction) {
            this.extensionUiBundlesToLoad = extensionUiBundlesToLoad;
            this.extensionUiLoadFunction = extensionUiLoadFunction;
        }

        @Override
        public void run() {
            while (!stopped) {
                try {
                    final Bundle bundle = extensionUiBundlesToLoad.poll(EXTENSION_UI_POLL_INTERVAL.getSeconds(), TimeUnit.SECONDS);
                    if (bundle != null) {
                        extensionUiLoadFunction.accept(bundle);
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (final Exception e) {
                    logger.error("Failed to load extension UI", e);
                }
            }
        }

        public void stop() {
            stopped = true;
        }
    }

}

