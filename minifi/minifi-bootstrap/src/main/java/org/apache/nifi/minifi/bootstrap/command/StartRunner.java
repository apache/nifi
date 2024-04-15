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

package org.apache.nifi.minifi.bootstrap.command;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.CMD_LOGGER;
import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.CONF_DIR_KEY;
import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.DEFAULT_LOGGER;
import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.STATUS_FILE_PID_KEY;
import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.UNINITIALIZED;
import static org.apache.nifi.minifi.bootstrap.Status.ERROR;
import static org.apache.nifi.minifi.bootstrap.Status.OK;
import static org.apache.nifi.minifi.bootstrap.service.MiNiFiExecCommandProvider.NIFI_BOOTSTRAP_LISTEN_PORT;
import static org.apache.nifi.minifi.commons.api.MiNiFiCommandState.FULLY_APPLIED;
import static org.apache.nifi.minifi.commons.api.MiNiFiCommandState.NOT_APPLIED_WITH_RESTART;
import static org.apache.nifi.minifi.commons.api.MiNiFiConstants.RAW_EXTENSION;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.io.FilenameUtils;
import org.apache.nifi.minifi.bootstrap.MiNiFiParameters;
import org.apache.nifi.minifi.bootstrap.RunMiNiFi;
import org.apache.nifi.minifi.bootstrap.ShutdownHook;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeException;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeListener;
import org.apache.nifi.minifi.bootstrap.exception.StartupFailureException;
import org.apache.nifi.minifi.bootstrap.service.BootstrapFileProvider;
import org.apache.nifi.minifi.bootstrap.service.CurrentPortProvider;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiExecCommandProvider;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiListener;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiPropertiesGenerator;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiStdLogHandler;
import org.apache.nifi.minifi.bootstrap.service.PeriodicStatusReporterManager;
import org.apache.nifi.minifi.commons.api.MiNiFiProperties;
import org.apache.nifi.minifi.properties.BootstrapProperties;

public class StartRunner implements CommandRunner {

    private static final int STARTUP_WAIT_SECONDS = 60;

    private final CurrentPortProvider currentPortProvider;
    private final BootstrapFileProvider bootstrapFileProvider;
    private final PeriodicStatusReporterManager periodicStatusReporterManager;
    private final MiNiFiStdLogHandler miNiFiStdLogHandler;
    private final MiNiFiParameters miNiFiParameters;
    private final File bootstrapConfigFile;
    private final Lock lock = new ReentrantLock();
    private final Condition startupCondition = lock.newCondition();
    private final RunMiNiFi runMiNiFi;
    private final MiNiFiExecCommandProvider miNiFiExecCommandProvider;
    private final ConfigurationChangeListener configurationChangeListener;
    private final MiNiFiPropertiesGenerator miNiFiPropertiesGenerator;

    private volatile ShutdownHook shutdownHook;

    private int listenPort;

    public StartRunner(CurrentPortProvider currentPortProvider, BootstrapFileProvider bootstrapFileProvider,
                       PeriodicStatusReporterManager periodicStatusReporterManager, MiNiFiStdLogHandler miNiFiStdLogHandler,
                       MiNiFiParameters miNiFiParameters, File bootstrapConfigFile, RunMiNiFi runMiNiFi,
                       MiNiFiExecCommandProvider miNiFiExecCommandProvider, ConfigurationChangeListener configurationChangeListener) {
        this.currentPortProvider = currentPortProvider;
        this.bootstrapFileProvider = bootstrapFileProvider;
        this.periodicStatusReporterManager = periodicStatusReporterManager;
        this.miNiFiStdLogHandler = miNiFiStdLogHandler;
        this.miNiFiParameters = miNiFiParameters;
        this.bootstrapConfigFile = bootstrapConfigFile;
        this.runMiNiFi = runMiNiFi;
        this.miNiFiExecCommandProvider = miNiFiExecCommandProvider;
        this.configurationChangeListener = configurationChangeListener;
        this.miNiFiPropertiesGenerator = new MiNiFiPropertiesGenerator();
    }

    /**
     * Starts (and restarts) MiNiFi process during the whole lifecycle of the bootstrap process.
     *
     * @param args the input arguments
     * @return status code
     */
    @Override
    public int runCommand(String[] args) {
        try {
            start();
        } catch (Exception e) {
            CMD_LOGGER.error("Exception happened during MiNiFi startup", e);
            return ERROR.getStatusCode();
        }
        return OK.getStatusCode();
    }

    private void start() throws IOException, InterruptedException, ConfigurationChangeException {
        Integer port = currentPortProvider.getCurrentPort();
        if (port != null) {
            CMD_LOGGER.info("Apache MiNiFi is already running, listening to Bootstrap on port {}", port);
            return;
        }

        File prevLockFile = bootstrapFileProvider.getLockFile();
        if (prevLockFile.exists() && !prevLockFile.delete()) {
            CMD_LOGGER.warn("Failed to delete previous lock file {}; this file should be cleaned up manually", prevLockFile);
        }

        BootstrapProperties bootstrapProperties = bootstrapFileProvider.getBootstrapProperties();
        String confDir = bootstrapProperties.getProperty(CONF_DIR_KEY);

        generateMiNiFiProperties(bootstrapFileProvider.getProtectedBootstrapProperties(), confDir);
        regenerateFlowConfiguration(bootstrapProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_FLOW_CONFIG.getKey()));

        Process process = startMiNiFi(bootstrapProperties);
        try {
            while (true) {
                if (process.isAlive()) {
                    handleReload();
                } else {
                    Runtime runtime = Runtime.getRuntime();
                    try {
                        runtime.removeShutdownHook(shutdownHook);
                    } catch (IllegalStateException ise) {
                        DEFAULT_LOGGER.trace("The virtual machine is already in the process of shutting down", ise);
                    }

                    if (runMiNiFi.isAutoRestartNiFi() && needRestart()) {
                        File reloadFile = bootstrapFileProvider.getReloadLockFile();
                        if (reloadFile.exists()) {
                            DEFAULT_LOGGER.info("Currently reloading configuration. Will wait to restart MiNiFi.");
                            Thread.sleep(5000L);
                            continue;
                        }
                        process = restartMiNifi(confDir);
                        // failed to start process
                        if (process == null) {
                            return;
                        }
                    } else {
                        return;
                    }
                }
            }
        } finally {
            miNiFiStdLogHandler.shutdown();
            runMiNiFi.shutdownChangeNotifier();
            periodicStatusReporterManager.shutdownPeriodicStatusReporters();
        }
    }

    private Process restartMiNifi(String confDir) throws IOException {
        Process process;
        boolean previouslyStarted = runMiNiFi.isNiFiStarted();
        boolean configChangeSuccessful = true;
        if (!previouslyStarted) {
            File bootstrapSwapConfigFile = bootstrapFileProvider.getBootstrapConfSwapFile();
            if (bootstrapSwapConfigFile.exists()) {
                if (!revertBootstrapConfig(confDir, bootstrapSwapConfigFile)) {
                    return null;
                }
            } else {
                DEFAULT_LOGGER.info("MiNiFi either never started or failed to restart. Will not attempt to restart MiNiFi");
                return null;
            }
            configChangeSuccessful = false;
        } else {
            runMiNiFi.setNiFiStarted(false);
        }

        miNiFiParameters.setSecretKey(null);

        CMD_LOGGER.info("Restarting Apache MiNiFi...");
        process = startMiNiFiProcess(getProcessBuilder());

        boolean started = waitForStart();

        final long pid = process.pid();
        if (started) {
            runMiNiFi.sendAcknowledgeToMiNiFi(configChangeSuccessful ? FULLY_APPLIED : NOT_APPLIED_WITH_RESTART);
            DEFAULT_LOGGER.info("Application Process [{}] started", pid);
        } else {
            DEFAULT_LOGGER.info("Application Process [{}] not started", pid);
        }
        return process;
    }

    private boolean revertBootstrapConfig(String confDir, File bootstrapSwapConfigFile) throws IOException {
        DEFAULT_LOGGER.info("Bootstrap Swap file exists, MiNiFi failed trying to change configuration. Reverting to old configuration.");

        Files.copy(bootstrapSwapConfigFile.toPath(), bootstrapConfigFile.toPath(), REPLACE_EXISTING);
        try {
            miNiFiPropertiesGenerator.generateMinifiProperties(confDir, bootstrapFileProvider.getBootstrapProperties());
        } catch (ConfigurationChangeException e) {
            DEFAULT_LOGGER.error("Unable to create MiNiFi properties file. Will not attempt to restart MiNiFi. Swap File should be cleaned up manually.");
            return false;
        }

        if (!bootstrapSwapConfigFile.delete()) {
            DEFAULT_LOGGER.warn("The bootstrap swap file failed to delete after replacing using it to revert to the old configuration. It should be cleaned up manually.");
        }
        runMiNiFi.setReloading(false);
        return true;
    }

    private boolean needRestart() throws IOException {
        boolean needRestart = true;
        File statusFile = bootstrapFileProvider.getStatusFile();
        if (!statusFile.exists()) {
            DEFAULT_LOGGER.info("Status File no longer exists. Will not restart MiNiFi");
            return false;
        }

        File lockFile = bootstrapFileProvider.getLockFile();
        if (lockFile.exists()) {
            DEFAULT_LOGGER.info("A shutdown was initiated. Will not restart MiNiFi");
            return false;
        }
        return needRestart;
    }

    private void handleReload() {
        try {
            Thread.sleep(1000L);
            if (runMiNiFi.getReloading() && runMiNiFi.isNiFiStarted()) {
                deleteSwapFile(bootstrapFileProvider.getBootstrapConfSwapFile());
                runMiNiFi.setReloading(false);
            }
        } catch (InterruptedException ie) {
            DEFAULT_LOGGER.warn("Thread interrupted while handling reload");
        }
    }

    private void deleteSwapFile(File file) {
        if (file.exists()) {
            DEFAULT_LOGGER.info("MiNiFi has finished reloading successfully and {} file exists. Deleting old configuration.", file.getName());

            if (file.delete()) {
                DEFAULT_LOGGER.info("Swap file ({}) was successfully deleted.", file.getName());
            } else {
                DEFAULT_LOGGER.error("Swap file ({}) was not deleted. It should be deleted manually.", file.getAbsoluteFile());
            }
        }
    }

    private void generateMiNiFiProperties(BootstrapProperties bootstrapProperties, String confDir) {
        DEFAULT_LOGGER.debug("Generating minifi.properties from bootstrap.conf");
        try {
            miNiFiPropertiesGenerator.generateMinifiProperties(confDir, bootstrapProperties);
        } catch (ConfigurationChangeException e) {
            throw new StartupFailureException("Unable to create MiNiFi properties file", e);
        }
    }

    private void regenerateFlowConfiguration(String flowConfigFileLocation) throws ConfigurationChangeException, IOException {
        Path flowConfigFile = Path.of(flowConfigFileLocation).toAbsolutePath();
        String currentFlowConfigFileBaseName = FilenameUtils.getBaseName(flowConfigFile.toString());
        Path rawConfigFile = flowConfigFile.getParent().resolve(currentFlowConfigFileBaseName + RAW_EXTENSION);
        if (Files.exists(rawConfigFile)) {
            DEFAULT_LOGGER.debug("Regenerating flow configuration {} from raw flow configuration {}", flowConfigFile, rawConfigFile);
            configurationChangeListener.handleChange(Files.newInputStream(rawConfigFile));
        }
    }

    private Process startMiNiFi(BootstrapProperties bootstrapProperties) throws IOException {
        int bootstrapListenPort = parseBootstrapListenPort(bootstrapProperties);

        MiNiFiListener listener = new MiNiFiListener();
        listenPort = listener.start(runMiNiFi, bootstrapListenPort, bootstrapFileProvider, configurationChangeListener);

        CMD_LOGGER.info("Starting Apache MiNiFi...");

        return startMiNiFiProcess(getProcessBuilder());
    }

    private int parseBootstrapListenPort(BootstrapProperties bootstrapProperties) {
        String bootstrapListenPort = bootstrapProperties.getProperty(NIFI_BOOTSTRAP_LISTEN_PORT);
        int parsedBootstrapListenPort = Optional.ofNullable(bootstrapListenPort)
            .map(String::trim)
            .map(port -> {
                try {
                    return Integer.parseInt(port);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Unable to parse Bootstrap listen port, please provide a valid port for " + NIFI_BOOTSTRAP_LISTEN_PORT);
                }
            })
            .orElse(0);
        CMD_LOGGER.info("Boostrap listen port {}", parsedBootstrapListenPort);
        return parsedBootstrapListenPort;
    }

    private ProcessBuilder getProcessBuilder() throws IOException {
        ProcessBuilder builder = new ProcessBuilder();
        File workingDir = getWorkingDir();

        List<String> cmd = miNiFiExecCommandProvider.getMiNiFiExecCommand(listenPort, workingDir);

        builder.command(cmd);
        builder.directory(workingDir);
        CMD_LOGGER.debug("Working Directory: {}", workingDir.getAbsolutePath());
        CMD_LOGGER.info("Command: {}", String.join(" ", cmd));
        return builder;
    }

    private Process startMiNiFiProcess(final ProcessBuilder builder) throws IOException {
        final Process process = builder.start();
        miNiFiStdLogHandler.initLogging(process);
        miNiFiParameters.setMiNiFiPort(UNINITIALIZED);
        miNiFiParameters.setMinifiPid(UNINITIALIZED);
        final long pid = process.pid();
        miNiFiParameters.setMinifiPid(pid);
        final Properties minifiProps = new Properties();
        minifiProps.setProperty(STATUS_FILE_PID_KEY, String.valueOf(pid));
        bootstrapFileProvider.saveStatusProperties(minifiProps);

        shutdownHook = new ShutdownHook(runMiNiFi, miNiFiStdLogHandler);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        return process;
    }

    private File getWorkingDir() throws IOException {
        BootstrapProperties props = bootstrapFileProvider.getBootstrapProperties();
        File bootstrapConfigAbsoluteFile = bootstrapConfigFile.getAbsoluteFile();
        File binDir = bootstrapConfigAbsoluteFile.getParentFile();

        return Optional.ofNullable(props.getProperty("working.dir"))
            .map(File::new)
            .orElse(binDir.getParentFile());
    }

    private boolean waitForStart() {
        lock.lock();
        try {
            long startTime = System.nanoTime();
            while (miNiFiParameters.getMinifiPid() < 1 && miNiFiParameters.getMiNiFiPort() < 1 || !runMiNiFi.isNiFiStarted()) {
                DEFAULT_LOGGER.debug("Waiting MiNiFi to start Pid={}, port={}, isNifiStarted={}",
                    miNiFiParameters.getMinifiPid(), miNiFiParameters.getMiNiFiPort(), runMiNiFi.isNiFiStarted());
                try {
                    startupCondition.await(1, TimeUnit.SECONDS);
                } catch (InterruptedException ie) {
                    return false;
                }

                long waitNanos = System.nanoTime() - startTime;
                long waitSeconds = TimeUnit.NANOSECONDS.toSeconds(waitNanos);
                if (waitSeconds > STARTUP_WAIT_SECONDS) {
                    return false;
                }
            }
        } finally {
            lock.unlock();
        }
        return true;
    }

}
