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

package org.apache.nifi.minifi.bootstrap;

import static java.util.Collections.singleton;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.nifi.minifi.bootstrap.command.CommandRunnerFactory;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeCoordinator;
import org.apache.nifi.minifi.bootstrap.service.BootstrapFileProvider;
import org.apache.nifi.minifi.bootstrap.service.CurrentPortProvider;
import org.apache.nifi.minifi.bootstrap.service.GracefulShutdownParameterProvider;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiCommandSender;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiConfigurationChangeListener;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiExecCommandProvider;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiStatusProvider;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiStdLogHandler;
import org.apache.nifi.minifi.bootstrap.service.PeriodicStatusReporterManager;
import org.apache.nifi.minifi.bootstrap.service.ReloadService;
import org.apache.nifi.minifi.bootstrap.util.ProcessUtils;
import org.apache.nifi.minifi.bootstrap.util.UnixProcessUtils;
import org.apache.nifi.minifi.commons.api.MiNiFiCommandState;
import org.apache.nifi.minifi.commons.service.StandardFlowEnrichService;
import org.apache.nifi.minifi.commons.service.StandardFlowSerDeService;
import org.apache.nifi.minifi.properties.BootstrapProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * The class which bootstraps Apache MiNiFi. This class looks for the
 * bootstrap.conf file by looking in the following places (in order):</p>
 * <ol>
 * <li>Java System Property named
 * {@code org.apache.nifi.minifi.bootstrap.config.file}</li>
 * <li>${MINIFI_HOME}/./conf/bootstrap.conf, where ${MINIFI_HOME} references an
 * environment variable {@code MINIFI_HOME}</li>
 * <li>./conf/bootstrap.conf, where {@code ./} represents the working
 * directory.</li>
 * </ol>
 * <p>
 * If the {@code bootstrap.conf} file cannot be found, throws a {@code FileNotFoundException}.
 */
public class RunMiNiFi implements ConfigurationFileHolder {
    // used for logging initial info; these will be logged to console by default when the app is started
    public static final Logger CMD_LOGGER = LoggerFactory.getLogger("org.apache.nifi.minifi.bootstrap.Command");
    // used for logging all info. These by default will be written to the log file
    public static final Logger DEFAULT_LOGGER = LoggerFactory.getLogger(RunMiNiFi.class);

    public static final String CONF_DIR_KEY = "conf.dir";
    public static final String STATUS_FILE_PID_KEY = "pid";
    public static final int UNINITIALIZED = -1;
    private static final String STATUS_FILE_PORT_KEY = "port";
    private static final String STATUS_FILE_SECRET_KEY = "secret.key";
    private static final String ACKNOWLEDGE_OPERATION = "ACKNOWLEDGE_OPERATION";
    private static final String PROCESS_KILL_SUCCESS_CHECK_RETRIES_KEY = "process.kill.success.check.retries";
    private static final String PROCESS_KILL_SUCCESS_CHECK_RETRIES_DEFAULT = "30";

    private final BootstrapFileProvider bootstrapFileProvider;
    private final ConfigurationChangeCoordinator configurationChangeCoordinator;
    private final CommandRunnerFactory commandRunnerFactory;
    private final AtomicReference<ByteBuffer> currentConfigFileReference = new AtomicReference<>();
    private final MiNiFiParameters miNiFiParameters;
    private final PeriodicStatusReporterManager periodicStatusReporterManager;
    private final ReloadService reloadService;
    private volatile Boolean autoRestartNiFi = true;
    private volatile boolean nifiStarted;
    private final Lock startedLock = new ReentrantLock();
    // Is set to true after the MiNiFi instance shuts down in preparation to be reloaded. Will be set to false after MiNiFi is successfully started again.
    private final AtomicBoolean reloading = new AtomicBoolean(false);
    private final AtomicBoolean commandInProgress = new AtomicBoolean(false);
    private final MiNiFiCommandSender miNiFiCommandSender;
    private final CurrentPortProvider currentPortProvider;
    private final ObjectMapper objectMapper;

    public RunMiNiFi(File bootstrapConfigFile) throws IOException {
        bootstrapFileProvider = new BootstrapFileProvider(bootstrapConfigFile);
        objectMapper = getObjectMapper();
        Properties properties = bootstrapFileProvider.getStatusProperties();
        BootstrapProperties bootstrapProperties = bootstrapFileProvider.getBootstrapProperties();

        miNiFiParameters = new MiNiFiParameters(
            Optional.ofNullable(properties.getProperty(STATUS_FILE_PORT_KEY)).map(Integer::parseInt).orElse(UNINITIALIZED),
            Optional.ofNullable(properties.getProperty(STATUS_FILE_PID_KEY)).map(Integer::parseInt).orElse(UNINITIALIZED),
            properties.getProperty(STATUS_FILE_SECRET_KEY)
        );
        ProcessUtils processUtils = new UnixProcessUtils(Integer.parseInt(
            bootstrapProperties.getProperty(PROCESS_KILL_SUCCESS_CHECK_RETRIES_KEY, PROCESS_KILL_SUCCESS_CHECK_RETRIES_DEFAULT)));

        miNiFiCommandSender = new MiNiFiCommandSender(miNiFiParameters, objectMapper);
        MiNiFiStatusProvider miNiFiStatusProvider = new MiNiFiStatusProvider(miNiFiCommandSender, processUtils);
        periodicStatusReporterManager =
            new PeriodicStatusReporterManager(bootstrapProperties, miNiFiStatusProvider, miNiFiCommandSender, miNiFiParameters);
        MiNiFiConfigurationChangeListener configurationChangeListener = new MiNiFiConfigurationChangeListener(this, DEFAULT_LOGGER, bootstrapFileProvider,
            new StandardFlowEnrichService(bootstrapProperties), StandardFlowSerDeService.defaultInstance());
        configurationChangeCoordinator = new ConfigurationChangeCoordinator(bootstrapFileProvider, this, singleton(configurationChangeListener));

        currentPortProvider = new CurrentPortProvider(miNiFiCommandSender, miNiFiParameters, processUtils);
        GracefulShutdownParameterProvider gracefulShutdownParameterProvider = new GracefulShutdownParameterProvider(bootstrapFileProvider);
        reloadService = new ReloadService(bootstrapFileProvider, miNiFiParameters, miNiFiCommandSender, currentPortProvider, gracefulShutdownParameterProvider, this, processUtils);
        commandRunnerFactory = new CommandRunnerFactory(miNiFiCommandSender, currentPortProvider, miNiFiParameters, miNiFiStatusProvider, periodicStatusReporterManager,
            bootstrapFileProvider, new MiNiFiStdLogHandler(), bootstrapConfigFile, this, gracefulShutdownParameterProvider,
            new MiNiFiExecCommandProvider(bootstrapFileProvider), processUtils, configurationChangeListener);
    }

    public int run(BootstrapCommand command, String... args) {
        return commandRunnerFactory.getRunner(command).runCommand(args);
    }

    public static void main(String[] args) {
        if (args.length < 1 || args.length > 3) {
            printUsage();
            return;
        }

        Optional<BootstrapCommand> cmd = BootstrapCommand.fromString(args[0]);
        if (!cmd.isPresent()) {
            printUsage();
            return;
        }

        try {
            RunMiNiFi runMiNiFi = new RunMiNiFi(BootstrapFileProvider.getBootstrapConfFile());
            System.exit(runMiNiFi.run(cmd.get(), args));
        } catch (Exception e) {
            CMD_LOGGER.error("Exception happened during the bootstrap run, check logs for details");
            DEFAULT_LOGGER.error("", e);
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println();
        System.out.println("java org.apache.nifi.minifi.bootstrap.RunMiNiFi <command> [options]");
        System.out.println();
        System.out.println("Valid commands include:");
        System.out.println();
        System.out.println("Start : Start a new instance of Apache MiNiFi");
        System.out.println("Stop : Stop a running instance of Apache MiNiFi");
        System.out.println("Restart : Stop Apache MiNiFi, if it is running, and then start a new instance");
        System.out.println("Status : Determine if there is a running instance of Apache MiNiFi");
        System.out.println("Dump : Write a Thread Dump to the file specified by [options], or to the log if no file is given");
        System.out.println("Run : Start a new instance of Apache MiNiFi and monitor the Process, restarting if the instance dies");
        System.out.println("FlowStatus : Get the status of the MiNiFi flow. For usage, read the System Admin Guide 'FlowStatus Query Options' section.");
        System.out.println();
    }

    public void setMiNiFiParameters(int port, String secretKey) throws IOException {
        if (Optional.ofNullable(secretKey).filter(key -> key.equals(miNiFiParameters.getSecretKey())).isPresent() && miNiFiParameters.getMiNiFiPort() == port) {
            DEFAULT_LOGGER.debug("secretKey and port match with the known one, nothing to update");
            return;
        }

        miNiFiParameters.setMiNiFiPort(port);
        miNiFiParameters.setSecretKey(secretKey);

        Properties minifiProps = new Properties();
        long minifiPid = miNiFiParameters.getMinifiPid();
        if (minifiPid != UNINITIALIZED) {
            minifiProps.setProperty(STATUS_FILE_PID_KEY, String.valueOf(minifiPid));
        }
        minifiProps.setProperty(STATUS_FILE_PORT_KEY, String.valueOf(port));
        minifiProps.setProperty(STATUS_FILE_SECRET_KEY, secretKey);

        File statusFile = bootstrapFileProvider.getStatusFile();
        try {
            bootstrapFileProvider.saveStatusProperties(minifiProps);
        } catch (IOException ioe) {
            DEFAULT_LOGGER.warn("Apache MiNiFi has started but failed to persist MiNiFi Port information to {}", statusFile.getAbsolutePath(), ioe);
        }

        CMD_LOGGER.info("The thread to run Apache MiNiFi is now running and listening for Bootstrap requests on port {}", port);
    }

    public void reload() throws IOException {
        reloadService.reload();
    }

    public void setNiFiStarted(boolean nifiStarted) {
        startedLock.lock();
        try {
            this.nifiStarted = nifiStarted;
        } finally {
            startedLock.unlock();
        }
    }

    public boolean isNiFiStarted() {
        startedLock.lock();
        try {
            return this.nifiStarted;
        } finally {
            startedLock.unlock();
        }
    }

    public void shutdownChangeNotifier() {
        configurationChangeCoordinator.close();
    }

    public void sendAcknowledgeToMiNiFi(MiNiFiCommandState commandState) {
        try {
            if (commandInProgress.getAndSet(false)) {
                Integer currentPort = currentPortProvider.getCurrentPort();
                CMD_LOGGER.info("Sending acknowledge with state {} to MiNiFi on port {}", commandState, currentPort);
                miNiFiCommandSender.sendCommand(ACKNOWLEDGE_OPERATION, currentPort, commandState.name());
            }
        } catch (Exception e) {
            CMD_LOGGER.error("Failed to send Acknowledge to MiNiFi", e);
        }
    }

    public PeriodicStatusReporterManager getPeriodicStatusReporterManager() {
        return periodicStatusReporterManager;
    }

    public ConfigurationChangeCoordinator getConfigurationChangeCoordinator() {
        return configurationChangeCoordinator;
    }

    void setAutoRestartNiFi(boolean restart) {
        this.autoRestartNiFi = restart;
    }

    public Boolean isAutoRestartNiFi() {
        return autoRestartNiFi;
    }

    public boolean getReloading() {
        return reloading.get();
    }

    public void setReloading(boolean val) {
        reloading.set(val);
    }

    @Override
    public AtomicReference<ByteBuffer> getConfigFileReference() {
        return currentConfigFileReference;
    }

    private ObjectMapper getObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
        return objectMapper;
    }

    public void setCommandInProgress(boolean value) {
        commandInProgress.set(value);
    }
}
