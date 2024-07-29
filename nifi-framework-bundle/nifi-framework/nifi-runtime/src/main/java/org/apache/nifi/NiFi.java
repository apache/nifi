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
package org.apache.nifi;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.diagnostics.DiagnosticsDump;
import org.apache.nifi.nar.ExtensionMapping;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.nar.NarClassLoadersHolder;
import org.apache.nifi.nar.NarUnpackMode;
import org.apache.nifi.nar.NarUnpacker;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.util.DiagnosticUtils;
import org.apache.nifi.util.FileUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class NiFi implements NiFiEntryPoint {

    public static final String BOOTSTRAP_PORT_PROPERTY = "nifi.bootstrap.listen.port";
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");

    private static final Logger LOGGER = LoggerFactory.getLogger(NiFi.class);

    private final NiFiServer nifiServer;
    private final BootstrapListener bootstrapListener;
    private final NiFiProperties properties;

    private volatile boolean shutdown = false;

    public NiFi(final NiFiProperties properties)
            throws ClassNotFoundException, IOException, IllegalArgumentException {
        this(properties, ClassLoader.getSystemClassLoader());
    }

    public NiFi(final NiFiProperties properties, ClassLoader rootClassLoader)
            throws ClassNotFoundException, IOException, IllegalArgumentException {

        this.properties = properties;

        // There can only be one krb5.conf for the overall Java process so set this globally during
        // start up so that processors and our Kerberos authentication code don't have to set this
        final File kerberosConfigFile = properties.getKerberosConfigurationFile();
        if (kerberosConfigFile != null) {
            final String kerberosConfigFilePath = kerberosConfigFile.getAbsolutePath();
            LOGGER.debug("Setting java.security.krb5.conf to {}", kerberosConfigFilePath);
            System.setProperty("java.security.krb5.conf", kerberosConfigFilePath);
        }

        setDefaultUncaughtExceptionHandler();

        // register the shutdown hook
        addShutdownHook();

        final String bootstrapPort = System.getProperty(BOOTSTRAP_PORT_PROPERTY);
        if (bootstrapPort != null) {
            try {
                final int port = Integer.parseInt(bootstrapPort);

                if (port < 1 || port > 65535) {
                    throw new RuntimeException("Failed to start NiFi because system property '" + BOOTSTRAP_PORT_PROPERTY + "' is not a valid integer in the range 1 - 65535");
                }

                bootstrapListener = new BootstrapListener(this, port);
                bootstrapListener.start(properties.getDefaultListenerBootstrapPort());
            } catch (final NumberFormatException nfe) {
                throw new RuntimeException("Failed to start NiFi because system property '" + BOOTSTRAP_PORT_PROPERTY + "' is not a valid integer in the range 1 - 65535");
            }
        } else {
            LOGGER.info("NiFi started without Bootstrap Port information provided; will not listen for requests from Bootstrap");
            bootstrapListener = null;
        }

        // delete the web working dir - if the application does not start successfully
        // the web app directories might be in an invalid state. when this happens
        // jetty will not attempt to re-extract the war into the directory. by removing
        // the working directory, we can be assured that it will attempt to extract the
        // war every time the application starts.
        File webWorkingDir = properties.getWebWorkingDirectory();
        FileUtils.deleteFilesInDirectory(webWorkingDir, null, LOGGER, true, true);
        FileUtils.deleteFile(webWorkingDir, LOGGER, 3);

        // redirect JUL log events
        initLogging();

        final Bundle systemBundle = SystemBundle.create(properties, rootClassLoader);

        // expand the nars
        final NarUnpackMode unpackMode = properties.isUnpackNarsToUberJar() ? NarUnpackMode.UNPACK_TO_UBER_JAR : NarUnpackMode.UNPACK_INDIVIDUAL_JARS;
        final ExtensionMapping extensionMapping = NarUnpacker.unpackNars(properties, systemBundle, unpackMode);

        // load the extensions classloaders
        NarClassLoaders narClassLoaders = NarClassLoadersHolder.getInstance();

        narClassLoaders.init(rootClassLoader, properties.getFrameworkWorkingDirectory(), properties.getExtensionsWorkingDirectory(), true);

        // load the framework classloader
        final ClassLoader frameworkClassLoader = narClassLoaders.getFrameworkBundle().getClassLoader();
        if (frameworkClassLoader == null) {
            throw new IllegalStateException("Unable to find the framework NAR ClassLoader.");
        }

        final Set<Bundle> narBundles = narClassLoaders.getBundles();

        final long startTime = System.nanoTime();
        nifiServer = narClassLoaders.getServer();
        if (nifiServer == null) {
            throw new IllegalStateException("Unable to find a NiFiServer implementation.");
        }
        Thread.currentThread().setContextClassLoader(nifiServer.getClass().getClassLoader());
        // Filter out the framework NAR from being loaded by the NiFiServer
        nifiServer.initialize(properties,
                systemBundle,
                narBundles,
                extensionMapping);

        if (shutdown) {
            LOGGER.info("NiFi has been shutdown via NiFi Bootstrap. Will not start Controller");
        } else {
            nifiServer.start();

            if (bootstrapListener != null) {
                bootstrapListener.setNiFiLoaded(true);
                bootstrapListener.sendStartedStatus(true);
            }

            final long duration = System.nanoTime() - startTime;
            final double durationSeconds = TimeUnit.NANOSECONDS.toMillis(duration) / 1000.0;
            LOGGER.info("Started Application Controller in {} seconds ({} ns)", durationSeconds, duration);
        }
    }

    public NiFiServer getServer() {
        return nifiServer;
    }

    protected void setDefaultUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler((thread, exception) -> LOGGER.error("An Unknown Error Occurred in Thread {}", thread, exception));
    }

    protected void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                // shutdown the jetty server
                shutdownHook(false)
        ));
    }

    protected void initLogging() {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    private static ClassLoader createBootstrapClassLoader() {
        //Get list of files in bootstrap folder
        final List<URL> urls = new ArrayList<>();
        try (final Stream<Path> files = Files.list(Paths.get("lib/bootstrap"))) {
            files.forEach(p -> {
                try {
                    urls.add(p.toUri().toURL());
                } catch (final MalformedURLException mef) {
                    LOGGER.warn("Unable to load bootstrap library [{}]", p.getFileName(), mef);
                }
            });
        } catch (IOException ioe) {
            LOGGER.warn("Unable to access lib/bootstrap to create bootstrap classloader", ioe);
        }
        //Create the bootstrap classloader
        return new URLClassLoader(urls.toArray(new URL[0]), Thread.currentThread().getContextClassLoader());
    }

    public void shutdownHook(final boolean isReload) {
        try {
            runDiagnosticsOnShutdown();
            shutdown();
        } catch (final Throwable t) {
            LOGGER.warn("Problem occurred ensuring Jetty web server was properly terminated", t);
        }
    }

    private void runDiagnosticsOnShutdown() throws IOException {
        if (properties.isDiagnosticsOnShutdownEnabled()) {
            final String diagnosticDirectoryPath = properties.getDiagnosticsOnShutdownDirectory();
            final boolean isCreated = DiagnosticUtils.createDiagnosticDirectory(diagnosticDirectoryPath);
            if (isCreated) {
                LOGGER.debug("Diagnostic directory has successfully been created.");
            }
            while (DiagnosticUtils.isFileCountExceeded(diagnosticDirectoryPath, properties.getDiagnosticsOnShutdownMaxFileCount())
                    || DiagnosticUtils.isSizeExceeded(diagnosticDirectoryPath, DataUnit.parseDataSize(properties.getDiagnosticsOnShutdownDirectoryMaxSize(), DataUnit.B).longValue())) {
                final Path oldestFile = DiagnosticUtils.getOldestFile(diagnosticDirectoryPath);
                Files.delete(oldestFile);
            }
            final String fileName = String.format("%s/diagnostic-%s.log", diagnosticDirectoryPath, DATE_TIME_FORMATTER.format(LocalDateTime.now()));
            diagnose(new File(fileName), properties.isDiagnosticsOnShutdownVerbose());
        }
    }

    private void diagnose(final File file, final boolean verbose) throws IOException {
        final DiagnosticsDump diagnosticsDump = getServer().getDiagnosticsFactory().create(verbose);
        try (final OutputStream fileOutputStream = new FileOutputStream(file)) {
            diagnosticsDump.writeTo(fileOutputStream);
        }
    }


    protected void shutdown() {
        this.shutdown = true;

        LOGGER.info("Application Server shutdown started");
        if (nifiServer != null) {
            nifiServer.stop();
        }
        if (bootstrapListener != null) {
            bootstrapListener.stop();
        }
        LOGGER.info("Application Server shutdown completed");
    }

    /**
     * Main entry point of the application.
     *
     * @param args things which are ignored
     */
    public static void main(String[] args) {
        LOGGER.info("Launching NiFi...");
        try {
            NiFiProperties properties = loadProperties();
            new NiFi(properties);
        } catch (final Throwable t) {
            LOGGER.error("Failure to launch NiFi", t);
        }
    }

    protected static NiFiProperties loadProperties() {
        return loadProperties(createBootstrapClassLoader());
    }

    protected static NiFiProperties loadProperties(final ClassLoader bootstrapClassLoader) {
        NiFiProperties properties = initializeProperties(bootstrapClassLoader);
        properties.validate();
        return properties;
    }

    private static NiFiProperties initializeProperties(final ClassLoader boostrapLoader) {
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(boostrapLoader);

        try {
            final Class<?> propsLoaderClass = Class.forName("org.apache.nifi.properties.NiFiPropertiesLoader", true, boostrapLoader);
            final Object loaderInstance = propsLoaderClass.getConstructor().newInstance();
            final Method getMethod = propsLoaderClass.getMethod("get");
            final NiFiProperties properties = (NiFiProperties) getMethod.invoke(loaderInstance);
            LOGGER.info("Application Properties loaded [{}]", properties.size());
            return properties;
        } catch (final InstantiationException | InvocationTargetException wrappedException) {
            final String msg = "There was an issue loading properties";
            throw new IllegalArgumentException(msg, wrappedException.getCause() == null ? wrappedException : wrappedException.getCause());
        } catch (final IllegalAccessException | NoSuchMethodException | ClassNotFoundException reex) {
            final String msg = "Unable to access properties loader in the expected manner - apparent classpath or build issue";
            throw new IllegalArgumentException(msg, reex);
        } catch (final RuntimeException e) {
            final String msg = "There was an issue decrypting protected properties";
            throw new IllegalArgumentException(msg, e);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }
}
