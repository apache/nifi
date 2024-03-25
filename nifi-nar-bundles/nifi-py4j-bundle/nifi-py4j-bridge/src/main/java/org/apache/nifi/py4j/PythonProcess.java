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

package org.apache.nifi.py4j;

import org.apache.nifi.py4j.logging.LogLevelChangeListener;
import org.apache.nifi.py4j.logging.PythonLogLevel;
import org.apache.nifi.py4j.logging.StandardLogLevelChangeHandler;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.py4j.client.JavaObjectBindings;
import org.apache.nifi.py4j.client.NiFiPythonGateway;
import org.apache.nifi.py4j.client.StandardPythonClient;
import org.apache.nifi.py4j.server.NiFiGatewayServer;
import org.apache.nifi.python.ControllerServiceTypeLookup;
import org.apache.nifi.python.PythonController;
import org.apache.nifi.python.PythonProcessConfig;
import org.apache.nifi.python.PythonProcessorDetails;
import org.apache.nifi.python.processor.PythonProcessorAdapter;
import org.apache.nifi.python.processor.PythonProcessorBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.CallbackClient;
import py4j.GatewayServer;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

public class PythonProcess {
    private static final Logger logger = LoggerFactory.getLogger(PythonProcess.class);
    private static final String PYTHON_CONTROLLER_FILENAME = "Controller.py";

    private static final String LOG_READER_THREAD_NAME_FORMAT = "python-log-%d";

    private final PythonProcessConfig processConfig;
    private final ControllerServiceTypeLookup controllerServiceTypeLookup;
    private final File virtualEnvHome;
    private final String componentType;
    private final String componentId;
    private GatewayServer server;
    private PythonController controller;
    private Process process;
    private NiFiPythonGateway gateway;
    private final Map<String, Boolean> processorPrefersIsolation = new ConcurrentHashMap<>();
    private final Set<CreatedProcessor> createdProcessors = new CopyOnWriteArraySet<>();
    private volatile boolean shutdown = false;
    private volatile List<String> extensionDirs;
    private volatile String workDir;
    private Thread logReaderThread;
    private String logListenerId;


    public PythonProcess(final PythonProcessConfig processConfig, final ControllerServiceTypeLookup controllerServiceTypeLookup, final File virtualEnvHome,
                         final String componentType, final String componentId) {
        this.processConfig = processConfig;
        this.controllerServiceTypeLookup = controllerServiceTypeLookup;
        this.virtualEnvHome = virtualEnvHome;
        this.componentType = componentType;
        this.componentId = componentId;
    }

    /**
     * Returns the current Controller for the Python Process. Note that the Controller
     * may change if the Python Process dies and is restarted. As a result, the value should never be
     * cached and reused later.
     * @return the current Controller for the Python Process
     */
    PythonController getCurrentController() {
        return controller;
    }

    public synchronized void start() throws IOException {
        final ServerSocketFactory serverSocketFactory = ServerSocketFactory.getDefault();
        final SocketFactory socketFactory = SocketFactory.getDefault();

        final int timeoutMillis = (int) processConfig.getCommsTimeout().toMillis();
        final String authToken = generateAuthToken();
        final CallbackClient callbackClient = new CallbackClient(GatewayServer.DEFAULT_PYTHON_PORT, GatewayServer.defaultAddress(), authToken,
            50000L, TimeUnit.MILLISECONDS, socketFactory, false, timeoutMillis);

        final JavaObjectBindings bindings = new JavaObjectBindings();
        gateway = new NiFiPythonGateway(bindings, null, callbackClient);
        gateway.startup();

        server = new NiFiGatewayServer(gateway,
            0,
            GatewayServer.defaultAddress(),
            timeoutMillis,
            timeoutMillis,
            Collections.emptyList(),
            serverSocketFactory,
            authToken,
            componentType,
            componentId);
        server.start();

        final int listeningPort = server.getListeningPort();

        setupEnvironment();
        this.process = launchPythonProcess(listeningPort, authToken);
        this.process.onExit().thenAccept(this::handlePythonProcessDied);

        final String logReaderThreadName = LOG_READER_THREAD_NAME_FORMAT.formatted(process.pid());
        final Runnable logReaderCommand = new PythonProcessLogReader(process.inputReader(StandardCharsets.UTF_8));
        this.logReaderThread = Thread.ofVirtual().name(logReaderThreadName).start(logReaderCommand);

        final StandardPythonClient pythonClient = new StandardPythonClient(gateway);
        controller = pythonClient.getController();

        final long timeout = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(60L);
        Exception lastException = null;
        boolean pingSuccessful = false;
        while (System.currentTimeMillis() < timeout) {
            try {
                final String pingResponse = controller.ping();
                pingSuccessful = "pong".equals(pingResponse);

                if (pingSuccessful) {
                    break;
                } else {
                    logger.debug("Got unexpected response from Py4J Server during ping: {}", pingResponse);
                }
            } catch (final Exception e) {
                lastException = e;
                logger.debug("Failed to start Py4J Server", e);
            }

            try {
                Thread.sleep(50L);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        if (!pingSuccessful && lastException != null) {
            throw new RuntimeException("Failed to start Python Bridge", lastException);
        }

        logListenerId = Long.toString(process.pid());
        StandardLogLevelChangeHandler.getHandler().addListener(logListenerId, new PythonProcessLogLevelChangeListener());

        controller.setControllerServiceTypeLookup(controllerServiceTypeLookup);
        logger.info("Successfully started and pinged Python Server. Python Process = {}", process);
    }

    private void handlePythonProcessDied(final Process process) {
        if (isShutdown()) {
            // If shutdown, don't try to restart the Process
            logger.info("Python Process {} exited with code {}", process, process.exitValue());
            return;
        }

        final List<String> processorsInvolved = this.createdProcessors.stream()
            .map(coordinates -> "%s (%s)".formatted(coordinates.identifier(), coordinates.type()))
            .toList();

        logger.error("Python Process {} with Processors {} died unexpectedly with exit code {}. Restarting...", process, processorsInvolved, process.exitValue());
        long backoff = 1000L;
        while (!isShutdown()) {
            try {
                // Ensure that we clean up any resources
                killProcess();

                // Restart the Process and establish new communications
                start();

                // Ensure that we re-discover any extensions, as this is necessary in order to create Processors
                if (extensionDirs != null && workDir != null) {
                    discoverExtensions(extensionDirs, workDir);
                    recreateProcessors();
                }

                return;
            } catch (final Exception e) {
                // If we fail to restart the Python Process, we'll keep trying, as long as the Process isn't intentionally shutdown.
                logger.error("Failed to restart Python Process with Processors {}; will keep trying", processorsInvolved, e);

                try {
                    // Sleep to avoid constantly hitting resources that are potentially already constrained
                    Thread.sleep(backoff);

                    // Exponentially backoff, but cap at 60 seconds
                    backoff = Math.min(60000L, backoff * 2);
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    private String generateAuthToken() {
        final SecureRandom random = new SecureRandom();
        final byte[] bytes = new byte[20];
        random.nextBytes(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    private Process launchPythonProcess(final int listeningPort, final String authToken) throws IOException {
        final File pythonFrameworkDirectory = processConfig.getPythonFrameworkDirectory();
        final File pythonApiDirectory = new File(pythonFrameworkDirectory.getParentFile(), "api");
        final String pythonCommand = resolvePythonCommand();

        final File controllerPyFile = new File(pythonFrameworkDirectory, PYTHON_CONTROLLER_FILENAME);
        final ProcessBuilder processBuilder = new ProcessBuilder();

        final List<String> commands = new ArrayList<>();
        commands.add(pythonCommand);

        String pythonPath = pythonApiDirectory.getAbsolutePath();

        if (processConfig.isDebugController() && "Controller".equals(componentId)) {
            commands.add("-m");
            commands.add("debugpy");
            commands.add("--listen");
            commands.add(processConfig.getDebugHost() + ":" + processConfig.getDebugPort());
            commands.add("--log-to-stderr");

            pythonPath = pythonPath + File.pathSeparator + virtualEnvHome.getAbsolutePath();
        }

        commands.add(controllerPyFile.getAbsolutePath());
        processBuilder.command(commands);

        processBuilder.environment().put("JAVA_PORT", String.valueOf(listeningPort));
        processBuilder.environment().put("ENV_HOME", virtualEnvHome.getAbsolutePath());
        processBuilder.environment().put("PYTHONPATH", pythonPath);
        processBuilder.environment().put("PYTHON_CMD", pythonCommand);
        processBuilder.environment().put("AUTH_TOKEN", authToken);

        // Redirect error stream to standard output stream
        processBuilder.redirectErrorStream(true);

        logger.info("Launching Python Process {} {} with working directory {} to communicate with Java on Port {}",
            pythonCommand, controllerPyFile.getAbsolutePath(), virtualEnvHome, listeningPort);
        return processBuilder.start();
    }

    String resolvePythonCommand() throws IOException {
        final File pythonCmdFile = new File(processConfig.getPythonCommand());
        final String pythonCmd = pythonCmdFile.getName();

        // Find command directories according to standard Python venv conventions
        final File[] virtualEnvDirectories = virtualEnvHome.listFiles((file, name) -> file.isDirectory() && (name.equals("bin") || name.equals("Scripts")));

        final String commandExecutableDirectory;
        if (virtualEnvDirectories == null || virtualEnvDirectories.length == 0) {
            throw new IOException("Python binary directory could not be found in " + virtualEnvHome);
        } else if( virtualEnvDirectories.length == 1) {
            commandExecutableDirectory = virtualEnvDirectories[0].getName();
        } else {
            // Default to bin directory for macOS and Linux
            commandExecutableDirectory = "bin";
        }

        final File pythonCommandFile = new File(virtualEnvHome, commandExecutableDirectory + File.separator + pythonCmd);
        return pythonCommandFile.getAbsolutePath();
    }


    private void setupEnvironment() throws IOException {
        final File environmentCreationCompleteFile = new File(virtualEnvHome, "env-creation-complete.txt");
        if (environmentCreationCompleteFile.exists()) {
            logger.debug("Environment has already been created for {}; will not recreate", virtualEnvHome);
            return;
        }

        logger.info("Creating Python Virtual Environment {}", virtualEnvHome);

        Files.createDirectories(virtualEnvHome.toPath());

        final String pythonCommand = processConfig.getPythonCommand();
        final String environmentPath = virtualEnvHome.getAbsolutePath();

        final ProcessBuilder processBuilder = new ProcessBuilder(pythonCommand, "-m", "venv", environmentPath);
        processBuilder.directory(virtualEnvHome.getParentFile());

        final String command = String.join(" ", processBuilder.command());
        logger.debug("Creating Python Virtual Environment {} using command {}", virtualEnvHome, command);
        final Process process = processBuilder.start();

        final int result;
        try {
            result = process.waitFor();
        } catch (final InterruptedException e) {
            throw new IOException("Interrupted while waiting for Python virtual environment to be created");
        }

        if (result != 0) {
            throw new IOException("Failed to create Python Environment " + virtualEnvHome + ": process existed with code " + result);
        }

        if (processConfig.isDebugController() && "Controller".equals(componentId)) {
            installDebugPy();
        }

        // Create file so that we don't keep trying to recreate the virtual environment
        environmentCreationCompleteFile.createNewFile();
        logger.info("Successfully created Python Virtual Environment {}", virtualEnvHome);
    }

    private void installDebugPy() throws IOException {
        final String pythonCommand = processConfig.getPythonCommand();

        final ProcessBuilder processBuilder = new ProcessBuilder(pythonCommand, "-m", "pip", "install", "--no-cache-dir", "--upgrade", "debugpy", "--target",
            virtualEnvHome.getAbsolutePath());
        processBuilder.directory(virtualEnvHome);

        final String command = String.join(" ", processBuilder.command());
        logger.debug("Installing DebugPy to Virtual Env {} using command {}", virtualEnvHome, command);
        final Process process = processBuilder.start();

        final int result;
        try {
            result = process.waitFor();
        } catch (final InterruptedException e) {
            throw new IOException("Interrupted while waiting for DebugPy to be installed");
        }

        if (result != 0) {
            throw new IOException("Failed to install DebugPy for Python Environment " + virtualEnvHome + ": process existed with code " + result);
        }
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public void shutdown() {
        shutdown = true;
        logger.info("Shutting down Python Process {}", process);
        killProcess();
    }

    private synchronized void killProcess() {
        if (logListenerId != null) {
            StandardLogLevelChangeHandler.getHandler().removeListener(logListenerId);
        }

        if (server != null) {
            try {
                server.shutdown();
            } catch (final Exception e) {
                logger.error("Failed to cleanly shutdown Py4J server", e);
            }

            server = null;
        }

        if (gateway != null) {
            try {
                gateway.shutdown(true);
            } catch (final Exception e) {
                logger.error("Failed to cleanly shutdown Py4J Gateway", e);
            }

            gateway = null;
        }

        if (process != null) {
            try {
                process.destroyForcibly();
            } catch (final Exception e) {
                logger.error("Failed to cleanly shutdown Py4J process", e);
            }

            process = null;
        }

        if (logReaderThread != null) {
            logReaderThread.interrupt();
        }
    }

    public void discoverExtensions(final List<String> directories, final String workDirectory) {
        extensionDirs = new ArrayList<>(directories);
        workDir = workDirectory;
        controller.discoverExtensions(directories, workDirectory);
    }

    public PythonProcessorBridge createProcessor(final String identifier, final String type, final String version, final String workDirPath) {
        final ProcessorCreationWorkflow creationWorkflow = new ProcessorCreationWorkflow() {
            @Override
            public void downloadDependencies() {
                controller.downloadDependencies(type, version, workDirPath);
            }

            @Override
            public PythonProcessorAdapter createProcessor() {
                return controller.createProcessor(type, version, workDirPath);
            }
        };

        // Create a PythonProcessorDetails and then call getProcessorType and getProcessorVersion to ensure that the details are cached
        final PythonProcessorDetails processorDetails = controller.getProcessorDetails(type, version);
        try {
            final String processorType = processorDetails.getProcessorType();
            final String processorVersion = processorDetails.getProcessorVersion();

            final PythonProcessorBridge processorBridge = new StandardPythonProcessorBridge.Builder()
                .controller(controller)
                .creationWorkflow(creationWorkflow)
                .processorType(processorType)
                .processorVersion(processorVersion)
                .workingDirectory(processConfig.getPythonWorkingDirectory())
                .moduleFile(new File(controller.getModuleFile(type, version)))
                .build();

            final CreatedProcessor createdProcessor = new CreatedProcessor(identifier, type, processorBridge);
            createdProcessors.add(createdProcessor);

            return processorBridge;
        } finally {
            processorDetails.free();
        }
    }

    /**
     * Updates all Processor Bridges to use the new Controller. This will cause the Processor Bridges to re-initialize
     * themselves and recreate the Python Processors that they interact with.
     */
    private void recreateProcessors() {
        for (final CreatedProcessor createdProcessor : createdProcessors) {
            createdProcessor.processorBridge().replaceController(controller);
            logger.info("Recreated Processor {} ({}) in Python Process {}", createdProcessor.identifier(), createdProcessor.type(), process);
        }
    }


    public boolean containsIsolatedProcessor() {
        return processorPrefersIsolation.containsValue(Boolean.TRUE);
    }

    public boolean removeProcessor(final String identifier) {
        final Boolean prefersIsolation = processorPrefersIsolation.remove(identifier);
        return prefersIsolation != null;
    }

    public int getProcessorCount() {
        return processorPrefersIsolation.size();
    }

    public Map<String, Integer> getJavaObjectBindingCounts() {
        return gateway.getObjectBindings().getCountsPerClass();
    }

    private record CreatedProcessor(String identifier, String type, PythonProcessorBridge processorBridge) {}

    private class PythonProcessLogLevelChangeListener implements LogLevelChangeListener {
        /**
         * Publish log level changes to Python Controller with conversion from framework log level to Python log level
         *
         * @param loggerName Name of logger with updated level
         * @param logLevel New log level
         */
        @Override
        public void onLevelChange(final String loggerName, final LogLevel logLevel) {
            final PythonLogLevel pythonLogLevel = PythonLogLevel.valueOf(logLevel);
            controller.setLoggerLevel(loggerName, pythonLogLevel.getLevel());
        }
    }
}