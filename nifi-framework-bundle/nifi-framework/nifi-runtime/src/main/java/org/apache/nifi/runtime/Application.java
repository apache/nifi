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
package org.apache.nifi.runtime;

import org.apache.nifi.NiFiServer;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.nar.ExtensionMapping;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.nar.NarClassLoadersHolder;
import org.apache.nifi.nar.NarUnpackMode;
import org.apache.nifi.nar.NarUnpacker;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.runtime.command.DiagnosticsCommand;
import org.apache.nifi.runtime.command.ShutdownCommand;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;

/**
 * Application command encapsulates standard initialization and shutdown hook registration
 */
public class Application implements Runnable {
    private static final String SECURITY_KRB5_CONF_PROPERTY = "java.security.krb5.conf";

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    private static final String VERSION_UNKNOWN = "UNKNOWN";

    private static final String APPLICATION_VERSION = Objects.requireNonNullElse(Application.class.getPackage().getImplementationVersion(), VERSION_UNKNOWN);

    /**
     * Run application
     */
    @Override
    public void run() {
        logger.info("Starting NiFi {} using Java {} with PID {}", APPLICATION_VERSION, Runtime.version(), ProcessHandle.current().pid());
        final Instant started = Instant.now();

        try {
            run(started);
        } catch (final Throwable e) {
            logger.error("Starting NiFi failed", e);
        }
    }

    private void run(final Instant started) {
        final NiFiProperties properties = PropertiesProvider.readProperties();
        final Bundle systemBundle = createSystemBundle(properties);
        final NarUnpackMode unpackMode = properties.isUnpackNarsToUberJar() ? NarUnpackMode.UNPACK_TO_UBER_JAR : NarUnpackMode.UNPACK_INDIVIDUAL_JARS;
        final ExtensionMapping extensionMapping = NarUnpacker.unpackNars(properties, systemBundle, unpackMode);
        final NarClassLoaders narClassLoaders = initializeClassLoaders(properties, systemBundle.getClassLoader());

        final NiFiServer applicationServer = narClassLoaders.getServer();
        if (applicationServer == null) {
            logger.error("Server implementation of [{}] not found", NiFiServer.class.getName());
        } else {
            try {
                startServer(systemBundle, extensionMapping, narClassLoaders, applicationServer, started);
            } catch (final Throwable e) {
                logger.error("Start Server failed", e);
                applicationServer.stop();
            }
        }
    }

    private Bundle createSystemBundle(final NiFiProperties properties) {
        final ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
        final String narLibraryDirectory = properties.getProperty(NiFiProperties.NAR_LIBRARY_DIRECTORY);
        return SystemBundle.create(narLibraryDirectory, systemClassLoader);
    }

    private NarClassLoaders initializeClassLoaders(final NiFiProperties properties, final ClassLoader systemClassLoader) {
        final NarClassLoaders narClassLoaders = NarClassLoadersHolder.getInstance();

        final File frameworkWorkingDirectory = properties.getFrameworkWorkingDirectory();
        final File extensionsWorkingDirectory = properties.getExtensionsWorkingDirectory();

        try {
            narClassLoaders.init(systemClassLoader, frameworkWorkingDirectory, extensionsWorkingDirectory, true);
        } catch (final Exception e) {
            logger.error("NAR Class Loaders initialization failed", e);
        }

        return narClassLoaders;
    }

    private void startServer(
            final Bundle systemBundle,
            final ExtensionMapping extensionMapping,
            final NarClassLoaders narClassLoaders,
            final NiFiServer applicationServer,
            final Instant started
    ) {
        // Set Application Server Class Loader for subsequent operations
        final ClassLoader applicationServerClassLoader = narClassLoaders.getServer().getClass().getClassLoader();
        Thread.currentThread().setContextClassLoader(applicationServerClassLoader);

        // Read Properties from Framework NAR containing nifi-properties-loader for additional processing
        final Bundle frameworkBundle = narClassLoaders.getFrameworkBundle();
        final ClassLoader frameworkClassLoader = frameworkBundle.getClassLoader();
        final NiFiProperties properties = PropertiesProvider.readProperties(frameworkClassLoader);

        // Initialize Application Server with properties and extensions
        setKerberosConfiguration(properties);
        final Set<Bundle> narBundles = narClassLoaders.getBundles();
        applicationServer.initialize(properties, systemBundle, narBundles, extensionMapping);

        final ManagementServer managementServer = ManagementServerProvider.getManagementServer(applicationServer);

        // Start Application Server before Management Server
        applicationServer.start();
        managementServer.start();

        // Add Shutdown Hook after application started
        final Runnable diagnosticsCommand = new DiagnosticsCommand(properties, applicationServer);
        final Runnable shutdownCommand = new ShutdownCommand(applicationServer, managementServer, diagnosticsCommand);
        addShutdownHook(shutdownCommand);

        final Instant completed = Instant.now();
        final Duration duration = Duration.between(started, completed);
        final double durationSeconds = duration.toMillis() / 1000.0;
        logger.info("Started Application in {} seconds ({} ns)", durationSeconds, duration.toNanos());
    }

    private void addShutdownHook(final Runnable shutdownCommand) {
        final Thread shutdownHook = Thread.ofPlatform()
                .name(shutdownCommand.getClass().getSimpleName())
                .uncaughtExceptionHandler(new StandardUncaughtExceptionHandler())
                .unstarted(shutdownCommand);

        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    private void setKerberosConfiguration(final NiFiProperties properties) {
        final File kerberosConfigFile = properties.getKerberosConfigurationFile();
        if (kerberosConfigFile == null) {
            logger.debug("Application Kerberos Configuration not specified");
        } else {
            final String kerberosConfigFilePath = kerberosConfigFile.getAbsolutePath();
            // Set System Kerberos Configuration based on application properties
            System.setProperty(SECURITY_KRB5_CONF_PROPERTY, kerberosConfigFilePath);
        }
    }
}
