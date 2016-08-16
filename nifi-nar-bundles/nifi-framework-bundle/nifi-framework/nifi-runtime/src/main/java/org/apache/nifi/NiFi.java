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

import java.io.File;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.nifi.documentation.DocGenerator;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.ExtensionMapping;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.nar.NarUnpacker;
import org.apache.nifi.properties.NiFiPropertiesLoader;
import org.apache.nifi.properties.SensitivePropertyProtectionException;
import org.apache.nifi.util.FileUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

public class NiFi {

    private static final Logger logger = LoggerFactory.getLogger(NiFi.class);
    private static final String KEY_FLAG = "-k";
    private final NiFiServer nifiServer;
    private final BootstrapListener bootstrapListener;

    public static final String BOOTSTRAP_PORT_PROPERTY = "nifi.bootstrap.listen.port";
    private volatile boolean shutdown = false;

    public NiFi(final NiFiProperties properties)
            throws ClassNotFoundException, IOException, NoSuchMethodException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread t, final Throwable e) {
                logger.error("An Unknown Error Occurred in Thread {}: {}", t, e.toString());
                logger.error("", e);
            }
        });

        // register the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                // shutdown the jetty server
                shutdownHook();
            }
        }));

        final String bootstrapPort = System.getProperty(BOOTSTRAP_PORT_PROPERTY);
        if (bootstrapPort != null) {
            try {
                final int port = Integer.parseInt(bootstrapPort);

                if (port < 1 || port > 65535) {
                    throw new RuntimeException("Failed to start NiFi because system property '" + BOOTSTRAP_PORT_PROPERTY + "' is not a valid integer in the range 1 - 65535");
                }

                bootstrapListener = new BootstrapListener(this, port);
                bootstrapListener.start();
            } catch (final NumberFormatException nfe) {
                throw new RuntimeException("Failed to start NiFi because system property '" + BOOTSTRAP_PORT_PROPERTY + "' is not a valid integer in the range 1 - 65535");
            }
        } else {
            logger.info("NiFi started without Bootstrap Port information provided; will not listen for requests from Bootstrap");
            bootstrapListener = null;
        }

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

        // expand the nars
        final ExtensionMapping extensionMapping = NarUnpacker.unpackNars(properties);

        // load the extensions classloaders
        NarClassLoaders.getInstance().init(properties.getFrameworkWorkingDirectory(), properties.getExtensionsWorkingDirectory());

        // load the framework classloader
        final ClassLoader frameworkClassLoader = NarClassLoaders.getInstance().getFrameworkClassLoader();
        if (frameworkClassLoader == null) {
            throw new IllegalStateException("Unable to find the framework NAR ClassLoader.");
        }

        // discover the extensions
        ExtensionManager.discoverExtensions(NarClassLoaders.getInstance().getExtensionClassLoaders());
        ExtensionManager.logClassLoaderMapping();

        DocGenerator.generate(properties);

        // load the server from the framework classloader
        Thread.currentThread().setContextClassLoader(frameworkClassLoader);
        Class<?> jettyServer = Class.forName("org.apache.nifi.web.server.JettyServer", true, frameworkClassLoader);
        Constructor<?> jettyConstructor = jettyServer.getConstructor(NiFiProperties.class);

        final long startTime = System.nanoTime();
        nifiServer = (NiFiServer) jettyConstructor.newInstance(properties);
        nifiServer.setExtensionMapping(extensionMapping);

        if (shutdown) {
            logger.info("NiFi has been shutdown via NiFi Bootstrap. Will not start Controller");
        } else {
            nifiServer.start();

            if (bootstrapListener != null) {
                bootstrapListener.sendStartedStatus(true);
            }

            final long endTime = System.nanoTime();
            logger.info("Controller initialization took " + (endTime - startTime) + " nanoseconds.");
        }
    }

    protected void shutdownHook() {
        try {
            this.shutdown = true;

            logger.info("Initiating shutdown of Jetty web server...");
            if (nifiServer != null) {
                nifiServer.stop();
            }
            if (bootstrapListener != null) {
                bootstrapListener.stop();
            }
            logger.info("Jetty web server shutdown completed (nicely or otherwise).");
        } catch (final Throwable t) {
            logger.warn("Problem occurred ensuring Jetty web server was properly terminated due to " + t);
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
        final Runnable command = new Runnable() {
            @Override
            public void run() {
                final long curMillis = System.currentTimeMillis();
                final long difference = curMillis - lastTriggerMillis.get();
                final long millisOff = Math.abs(difference - 2000L);
                occurrences.incrementAndGet();
                if (millisOff > 500L) {
                    occurrencesOutOfRange.incrementAndGet();
                }
                lastTriggerMillis.set(curMillis);
            }
        };

        final ScheduledFuture<?> future = service.scheduleWithFixedDelay(command, 2000L, 2000L, TimeUnit.MILLISECONDS);

        final TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                future.cancel(true);
                service.shutdownNow();

                if (occurrences.get() < minRequiredOccurrences || occurrencesOutOfRange.get() > maxOccurrencesOutOfRange) {
                    logger.warn("NiFi has detected that this box is not responding within the expected timing interval, which may cause "
                            + "Processors to be scheduled erratically. Please see the NiFi documentation for more information.");
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
        logger.info("Launching NiFi...");
        try {
            NiFiProperties properties = initializeProperties(args);
            new NiFi(properties);
        } catch (final Throwable t) {
            logger.error("Failure to launch NiFi due to " + t, t);
        }
    }

    private static NiFiProperties initializeProperties(String[] args) {
        // Try to get key
        // If key doesn't exist, instantiate without
        // Load properties
        // If properties are protected and key missing, throw RuntimeException

        try {
            String key = loadFormattedKey(args);
            // The key might be empty or null when it is passed to the loader
            try {
                NiFiProperties properties = NiFiPropertiesLoader.withKey(key).get();
                logger.info("Loaded {} properties", properties.size());
                return properties;
            } catch (SensitivePropertyProtectionException e) {
                final String msg = "There was an issue decrypting protected properties";
                logger.error(msg, e);
                throw new IllegalArgumentException(msg);
            }
        } catch (IllegalArgumentException e) {
            final String msg = "The bootstrap process did not provide a valid key and there are protected properties present in the properties file";
            logger.error(msg, e);
            throw new IllegalArgumentException(msg);
        }
    }

    private static String loadFormattedKey(String[] args) {
        String key = null;
        List<String> parsedArgs = parseArgs(args);
        // Check if args contain protection key
        if (parsedArgs.contains(KEY_FLAG)) {
            key = getKeyFromArgs(parsedArgs);

            // Format the key (check hex validity and remove spaces)
            key = formatHexKey(key);
            if (!isHexKeyValid(key)) {
                throw new IllegalArgumentException("The key was not provided in valid hex format and of the correct length");
            }

            return key;
        } else {
            // throw new IllegalStateException("No key provided from bootstrap");
            return "";
        }
    }

    private static String getKeyFromArgs(List<String> parsedArgs) {
        String key;
        logger.debug("The bootstrap process provided the " + KEY_FLAG + " flag");
        int i = parsedArgs.indexOf(KEY_FLAG);
        if (parsedArgs.size() <= i + 1) {
            logger.error("The bootstrap process passed the {} flag without a key", KEY_FLAG);
            throw new IllegalArgumentException("The bootstrap process provided the " + KEY_FLAG + " flag but no key");
        }
        key = parsedArgs.get(i + 1);
        logger.info("Read property protection key from bootstrap process");
        return key;
    }

    private static List<String> parseArgs(String[] args) {
        List<String> parsedArgs = new ArrayList<>(Arrays.asList(args));
        for (int i = 0; i < parsedArgs.size(); i++) {
            if (parsedArgs.get(i).startsWith(KEY_FLAG + " ")) {
                String[] split = parsedArgs.get(i).split(" ", 2);
                parsedArgs.set(i, split[0]);
                parsedArgs.add(i + 1, split[1]);
                break;
            }
        }
        return parsedArgs;
    }

    private static String formatHexKey(String input) {
        if (input == null || input.trim().isEmpty()) {
            return "";
        }
        return input.replaceAll("[^0-9a-fA-F]", "").toLowerCase();
    }

    private static boolean isHexKeyValid(String key) {
        if (key == null || key.trim().isEmpty()) {
            return false;
        }
        // Key length is in "nibbles" (i.e. one hex char = 4 bits)
        return Arrays.asList(128, 196, 256).contains(key.length() * 4) && key.matches("^[0-9a-fA-F]*$");
    }
}
