/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.minifi;

import static org.apache.nifi.minifi.util.BootstrapClassLoaderUtils.createBootstrapClassLoader;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.nifi.NiFiServer;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.nar.ExtensionMapping;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.nar.NarClassLoadersHolder;
import org.apache.nifi.nar.NarUnpackMode;
import org.apache.nifi.nar.NarUnpacker;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.util.FileUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

public class MiNiFi {

    private static final Logger logger = LoggerFactory.getLogger(MiNiFi.class);

    private final MiNiFiServer minifiServer;
    private volatile boolean shutdown = false;

    private static final String FRAMEWORK_NAR_ID = "minifi-framework-nar";


    public MiNiFi(final NiFiProperties properties)
            throws ClassNotFoundException, IOException, IllegalArgumentException {
        this(properties, ClassLoader.getSystemClassLoader());
    }

    public MiNiFi(final NiFiProperties properties, ClassLoader rootClassLoader) throws ClassNotFoundException, IOException, IllegalArgumentException {

        // There can only be one krb5.conf for the overall Java process so set this globally during
        // start up so that processors and our Kerberos authentication code don't have to set this
        final File kerberosConfigFile = properties.getKerberosConfigurationFile();
        if (kerberosConfigFile != null) {
            final String kerberosConfigFilePath = kerberosConfigFile.getAbsolutePath();
            logger.info("Setting java.security.krb5.conf to {}", kerberosConfigFilePath);
            System.setProperty("java.security.krb5.conf", kerberosConfigFilePath);
        }

        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            logger.error("An Unknown Error Occurred in Thread {}", t, e);
        });

        // register the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // shutdown the jetty server
            shutdownHook(false);
        }));

        // delete the web working dir - if the application does not start successfully
        // the web app directories might be in an invalid state. when this happens
        // jetty will not attempt to re-extract the war into the directory. by removing
        // the working directory, we can be assured that it will attempt to extract the
        // war every time the application starts.
        File webWorkingDir = properties.getWebWorkingDirectory();
        FileUtils.deleteFilesInDirectory(webWorkingDir, null, logger, true, true);
        FileUtils.deleteFile(webWorkingDir, logger, 3);

        detectTimingIssues();

        // redirect JUL log events
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        final Bundle systemBundle = SystemBundle.create(properties, rootClassLoader);

        // expand the nars
        final NarUnpackMode unpackMode = properties.isUnpackNarsToUberJar() ? NarUnpackMode.UNPACK_TO_UBER_JAR : NarUnpackMode.UNPACK_INDIVIDUAL_JARS;
        final ExtensionMapping extensionMapping = NarUnpacker.unpackNars(properties, FRAMEWORK_NAR_ID, systemBundle, unpackMode);

        // load the extensions classloaders
        NarClassLoaders narClassLoaders = NarClassLoadersHolder.getInstance();
        narClassLoaders.init(rootClassLoader,
                properties.getFrameworkWorkingDirectory(), properties.getExtensionsWorkingDirectory(), FRAMEWORK_NAR_ID, true);

        /// load the framework classloader
        final ClassLoader frameworkClassLoader = narClassLoaders.getFrameworkBundle().getClassLoader();
        if (frameworkClassLoader == null) {
            throw new IllegalStateException("Unable to find the framework NAR ClassLoader.");
        }

        final Set<Bundle> narBundles = narClassLoaders.getBundles();

        final long startTime = System.nanoTime();
        final NiFiServer nifiServer = narClassLoaders.getServer();
        if (nifiServer == null) {
            throw new IllegalStateException("Unable to find a NiFiServer implementation.");
        }
        if (!(nifiServer instanceof MiNiFiServer)) {
            throw new IllegalStateException("Found NiFiServer implementation with class name " + nifiServer.getClass().getName()
                    + ", it does not implement the required MiNiFiServer interface.");
        }
        minifiServer = (MiNiFiServer) nifiServer;
        Thread.currentThread().setContextClassLoader(minifiServer.getClass().getClassLoader());

        // Filter out the framework NAR from being loaded by the NiFiServer
        minifiServer.initialize(properties,
                systemBundle,
                narBundles,
                extensionMapping);

        if (shutdown) {
            logger.info("MiNiFi has been shutdown via MiNiFi Bootstrap. Will not start Controller");
        } else {
            minifiServer.start();

            final long endTime = System.nanoTime();
            final long durationNanos = endTime - startTime;
            // Convert to millis for higher precision and then convert to a float representation of seconds
            final float durationSeconds = TimeUnit.MILLISECONDS.convert(durationNanos, TimeUnit.NANOSECONDS) / 1000f;
            logger.info("Controller initialization took {} nanoseconds ({} seconds).", durationNanos, String.format("%.01f", durationSeconds));
        }
    }

    protected void shutdownHook(boolean isReload) {
        try {
            this.shutdown = true;

            logger.info("Initiating shutdown of MiNiFi server...");
            if (minifiServer != null) {
                minifiServer.stop();
            }

            logger.info("MiNiFi server shutdown completed (nicely or otherwise).");
        } catch (final Throwable t) {
            logger.warn("Problem occurred ensuring MiNiFi server was properly terminated due to {}", t.getMessage());
        }
    }

    /**
     * Determine if the machine we're running on has timing issues.
     */
    private void detectTimingIssues() {
        final int minRequiredOccurrences = 25;
        final int maxOccurrencesOutOfRange = 15;
        final AtomicLong lastTriggerMillis = new AtomicLong(System.currentTimeMillis());

        final ScheduledExecutorService service = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(final Runnable r) {
                final Thread t = defaultFactory.newThread(r);
                t.setDaemon(true);
                t.setName("Detect Timing Issues");
                return t;
            }
        });

        final AtomicInteger occurrencesOutOfRange = new AtomicInteger(0);
        final AtomicInteger occurrences = new AtomicInteger(0);
        final Runnable command = () -> {
            final long curMillis = System.currentTimeMillis();
            final long difference = curMillis - lastTriggerMillis.get();
            final long millisOff = Math.abs(difference - 2000L);
            occurrences.incrementAndGet();
            if (millisOff > 500L) {
                occurrencesOutOfRange.incrementAndGet();
            }
            lastTriggerMillis.set(curMillis);
        };

        final ScheduledFuture<?> future = service.scheduleWithFixedDelay(command, 2000L, 2000L, TimeUnit.MILLISECONDS);

        final TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                future.cancel(true);
                service.shutdownNow();

                if (occurrences.get() < minRequiredOccurrences || occurrencesOutOfRange.get() > maxOccurrencesOutOfRange) {
                    logger.warn("MiNiFi has detected that this box is not responding within the expected timing interval, which may cause "
                            + "Processors to be scheduled erratically. Please see the MiNiFi documentation for more information.");
                }
            }
        };
        final Timer timer = new Timer(true);
        timer.schedule(timerTask, 60000L);
    }

    /**
     * Main entry point of the application.
     *
     * @param args things which are ignored
     */
    public static void main(String[] args) {
        logger.info("Launching MiNiFi...");
        try {
            NiFiProperties properties = getValidatedMiNifiProperties();
            new MiNiFi(properties);
        } catch (final Throwable t) {
            logger.error("Failure to launch MiNiFi", t);
            System.exit(1);
        }
    }

    protected static NiFiProperties getValidatedMiNifiProperties() {
        NiFiProperties properties = initializeProperties(createBootstrapClassLoader());
        properties.validate();
        return properties;
    }

    private static NiFiProperties initializeProperties(ClassLoader boostrapLoader) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

        Thread.currentThread().setContextClassLoader(boostrapLoader);

        try {
            Class<?> propsLoaderClass = Class.forName("org.apache.nifi.minifi.properties.MiNiFiPropertiesLoader", true, boostrapLoader);
            Constructor<?> constructor = propsLoaderClass.getConstructor();
            Object loaderInstance = constructor.newInstance();
            Method getMethod = propsLoaderClass.getMethod("get");
            NiFiProperties properties = (NiFiProperties) getMethod.invoke(loaderInstance);
            logger.info("Application Properties loaded [{}]", properties.size());
            return properties;
        } catch (InvocationTargetException wrappedException) {
            throw new IllegalArgumentException("There was an issue decrypting protected properties", wrappedException.getCause() == null ? wrappedException : wrappedException.getCause());
        } catch (final IllegalAccessException | NoSuchMethodException | ClassNotFoundException | InstantiationException reex) {
            throw new IllegalArgumentException("Unable to access properties loader in the expected manner - apparent classpath or build issue", reex);
        } catch (final RuntimeException e) {
            throw new IllegalArgumentException("There was an issue decrypting protected properties", e);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }
}
