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

import org.apache.nifi.py4j.client.JavaObjectBindings;
import org.apache.nifi.py4j.client.NiFiPythonGateway;
import org.apache.nifi.py4j.client.StandardPythonClient;
import org.apache.nifi.py4j.server.NiFiGatewayServer;
import org.apache.nifi.python.ControllerServiceTypeLookup;
import org.apache.nifi.python.PythonController;
import org.apache.nifi.python.PythonProcessConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.CallbackClient;
import py4j.GatewayServer;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

// TODO / Figure Out for MVP:
//      MUST DO:
//      - Documentation
//          - Developer Guide
//              - Controller Services
//                  - Need to update docs to show the interfaces that are exposed, explain how to get these...
//      - Setup proper logging on the Python side: https://docs.python.org/2/howto/logging-cookbook.html#using-file-rotation
//      - For FlowFileTransform, allow the result to contain either a byte array or a String. If a String, just convert in the parent class.
//      - Figure out how to deal with Python Packaging
//              - Need to figure out how to deal with additionalDetails.html, docs directory in python project typically?
//              - Understand how to deal with versioning
//      - Performance concern for TransformRecord
//              - Currently, triggering the transform() method is pretty fast. But then the Result object comes back and we have to call into the Python side to call the getters
//                over and over. Need to look into instead serializing the entire response as JSON and sending that back.
//              - Also, since this is heavy JSON processing, might want to consider ORJSON or something like that instead of inbuilt JSON parser/generator
//      - Test pip install nifi-my-proc, does nifi pick it up?
//      - When ran DetectObjectInImage with multiple threads, Python died. Need to figure out why.
//      - If Python Process dies, need to create a new process and need to then create all of the Processors that were in that Process and initialize them.
//            - Milestone 2 or 3, not Milestone 1.
//      - Additional Interfaces beyond just FlowFileTransform
//          - FlowFileSource
//      - Restructure Maven projects
//          - Should this all go under Framework?
//
//
//      CONSIDER:
//      - Clustering: Ensure component on all nodes?
//          - Consider "pip freeze" type of thing to ensure that python dependencies are same across nodes when joining cluster.
//

public class PythonProcess {
    private static final Logger logger = LoggerFactory.getLogger(PythonProcess.class);
    private static final String PYTHON_CONTROLLER_FILENAME = "Controller.py";

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


    public PythonProcess(final PythonProcessConfig processConfig, final ControllerServiceTypeLookup controllerServiceTypeLookup, final File virtualEnvHome,
                         final String componentType, final String componentId) {
        this.processConfig = processConfig;
        this.controllerServiceTypeLookup = controllerServiceTypeLookup;
        this.virtualEnvHome = virtualEnvHome;
        this.componentType = componentType;
        this.componentId = componentId;
    }

    public PythonController getController() {
        return controller;
    }

    public void start() throws IOException {
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

        controller.setControllerServiceTypeLookup(controllerServiceTypeLookup);
        logger.info("Successfully started and pinged Python Server. Python Process = {}", process);
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
        final File pythonLogsDirectory = processConfig.getPythonLogsDirectory();
        final File pythonCmdFile = new File(processConfig.getPythonCommand());
        final String pythonCmd = pythonCmdFile.getName();
        final File pythonCommandFile = new File(virtualEnvHome, "bin/" + pythonCmd);
        final String pythonCommand = pythonCommandFile.getAbsolutePath();

        final File controllerPyFile = new File(pythonFrameworkDirectory, PYTHON_CONTROLLER_FILENAME);
        final ProcessBuilder processBuilder = new ProcessBuilder(pythonCommand, controllerPyFile.getAbsolutePath());
        processBuilder.environment().put("JAVA_PORT", String.valueOf(listeningPort));
        processBuilder.environment().put("LOGS_DIR", pythonLogsDirectory.getAbsolutePath());
        processBuilder.environment().put("ENV_HOME", virtualEnvHome.getAbsolutePath());
        processBuilder.environment().put("PYTHONPATH", pythonApiDirectory.getAbsolutePath());
        processBuilder.environment().put("PYTHON_CMD", pythonCommandFile.getAbsolutePath());
        processBuilder.environment().put("AUTH_TOKEN", authToken);
        processBuilder.inheritIO();

        logger.info("Launching Python Process {} {} with working directory {} to communicate with Java on Port {}",
            pythonCommand, controllerPyFile.getAbsolutePath(), virtualEnvHome, listeningPort);
        return processBuilder.start();
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

        // Create file so that we don't keep trying to recreate the virtual environment
        environmentCreationCompleteFile.createNewFile();
        logger.info("Successfully created Python Virtual Environment {}", virtualEnvHome);
    }

    public void shutdown() {
        logger.info("Shutting down Python Process {}", process);

        if (server != null) {
            try {
                server.shutdown();
            } catch (final Exception e) {
                logger.error("Failed to cleanly shutdown Py4J server", e);
            }
        }

        if (gateway != null) {
            try {
                gateway.shutdown(true);
            } catch (final Exception e) {
                logger.error("Failed to cleanly shutdown Py4J Gateway", e);
            }
        }

        if (process != null) {
            try {
                process.destroyForcibly();
            } catch (final Exception e) {
                logger.error("Failed to cleanly shutdown Py4J process", e);
            }
        }
    }

    void addProcessor(final String identifier, final boolean prefersIsolation) {
        processorPrefersIsolation.put(identifier, prefersIsolation);
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
}
