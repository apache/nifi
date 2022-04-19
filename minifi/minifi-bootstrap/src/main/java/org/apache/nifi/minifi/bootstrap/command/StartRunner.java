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
import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.MINIFI_CONFIG_FILE_KEY;
import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.STATUS_FILE_PID_KEY;
import static org.apache.nifi.minifi.bootstrap.Status.ERROR;
import static org.apache.nifi.minifi.bootstrap.Status.OK;
import static org.apache.nifi.minifi.bootstrap.util.ConfigTransformer.generateConfigFiles;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.nifi.bootstrap.util.OSUtils;
import org.apache.nifi.minifi.bootstrap.MiNiFiParameters;
import org.apache.nifi.minifi.bootstrap.RunMiNiFi;
import org.apache.nifi.minifi.bootstrap.ShutdownHook;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeException;
import org.apache.nifi.minifi.bootstrap.exception.StartupFailureException;
import org.apache.nifi.minifi.bootstrap.service.BootstrapFileProvider;
import org.apache.nifi.minifi.bootstrap.service.CurrentPortProvider;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiExecCommandProvider;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiListener;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiStdLogHandler;
import org.apache.nifi.minifi.bootstrap.service.PeriodicStatusReporterManager;
import org.apache.nifi.minifi.bootstrap.util.UnixProcessUtils;
import org.apache.nifi.util.Tuple;

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
    private volatile ShutdownHook shutdownHook;
    private final MiNiFiExecCommandProvider miNiFiExecCommandProvider;

    public StartRunner(CurrentPortProvider currentPortProvider, BootstrapFileProvider bootstrapFileProvider,
        PeriodicStatusReporterManager periodicStatusReporterManager, MiNiFiStdLogHandler miNiFiStdLogHandler, MiNiFiParameters miNiFiParameters, File bootstrapConfigFile,
        RunMiNiFi runMiNiFi, MiNiFiExecCommandProvider miNiFiExecCommandProvider) {
        this.currentPortProvider = currentPortProvider;
        this.bootstrapFileProvider = bootstrapFileProvider;
        this.periodicStatusReporterManager = periodicStatusReporterManager;
        this.miNiFiStdLogHandler = miNiFiStdLogHandler;
        this.miNiFiParameters = miNiFiParameters;
        this.bootstrapConfigFile = bootstrapConfigFile;
        this.runMiNiFi = runMiNiFi;
        this.miNiFiExecCommandProvider = miNiFiExecCommandProvider;
    }

    /**
     * Starts (and restarts) MiNiFi process during the whole lifecycle of the bootstrap process.
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

    private void start() throws IOException, InterruptedException {
        Integer port = currentPortProvider.getCurrentPort();
        if (port != null) {
            CMD_LOGGER.info("Apache MiNiFi is already running, listening to Bootstrap on port {}", port);
            return;
        }

        File prevLockFile = bootstrapFileProvider.getLockFile();
        if (prevLockFile.exists() && !prevLockFile.delete()) {
            CMD_LOGGER.warn("Failed to delete previous lock file {}; this file should be cleaned up manually", prevLockFile);
        }

        Properties bootstrapProperties = bootstrapFileProvider.getBootstrapProperties();
        String confDir = bootstrapProperties.getProperty(CONF_DIR_KEY);
        initConfigFiles(bootstrapProperties, confDir);

        Tuple<ProcessBuilder, Process> tuple = startMiNiFi();
        ProcessBuilder builder = tuple.getKey();
        Process process = tuple.getValue();

        try {
            while (true) {
                if (UnixProcessUtils.isAlive(process)) {
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

                        process = restartNifi(bootstrapProperties, confDir, builder, runtime);
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

    private Process restartNifi(Properties bootstrapProperties, String confDir, ProcessBuilder builder, Runtime runtime) throws IOException {
        Process process;
        boolean previouslyStarted = runMiNiFi.isNiFiStarted();
        if (!previouslyStarted) {
            File swapConfigFile = bootstrapFileProvider.getSwapFile();
            if (swapConfigFile.exists()) {
                DEFAULT_LOGGER.info("Swap file exists, MiNiFi failed trying to change configuration. Reverting to old configuration.");

                try {
                    ByteBuffer tempConfigFile = generateConfigFiles(new FileInputStream(swapConfigFile), confDir, bootstrapProperties);
                    runMiNiFi.getConfigFileReference().set(tempConfigFile.asReadOnlyBuffer());
                } catch (ConfigurationChangeException e) {
                    DEFAULT_LOGGER.error("The swap file is malformed, unable to restart from prior state. Will not attempt to restart MiNiFi. Swap File should be cleaned up manually.");
                    return null;
                }

                Files.copy(swapConfigFile.toPath(), Paths.get(bootstrapProperties.getProperty(MINIFI_CONFIG_FILE_KEY)), REPLACE_EXISTING);

                DEFAULT_LOGGER.info("Replacing config file with swap file and deleting swap file");
                if (!swapConfigFile.delete()) {
                    DEFAULT_LOGGER.warn("The swap file failed to delete after replacing using it to revert to the old configuration. It should be cleaned up manually.");
                }
                runMiNiFi.setReloading(false);
            } else {
                DEFAULT_LOGGER.info("MiNiFi either never started or failed to restart. Will not attempt to restart MiNiFi");
                return null;
            }
        } else {
            runMiNiFi.setNiFiStarted(false);
        }

        miNiFiParameters.setSecretKey(null);

        process = startMiNiFiProcess(builder);

        boolean started = waitForStart();

        if (started) {
            Long pid = OSUtils.getProcessId(process, DEFAULT_LOGGER);
            DEFAULT_LOGGER.info("Successfully spawned the thread to start Apache MiNiFi{}", (pid == null ? "" : " with PID " + pid));
        } else {
            DEFAULT_LOGGER.error("Apache MiNiFi does not appear to have started");
        }
        return process;
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
                File swapConfigFile = bootstrapFileProvider.getSwapFile();
                if (swapConfigFile.exists()) {
                    DEFAULT_LOGGER.info("MiNiFi has finished reloading successfully and swap file exists. Deleting old configuration.");

                    if (swapConfigFile.delete()) {
                        DEFAULT_LOGGER.info("Swap file was successfully deleted.");
                    } else {
                        DEFAULT_LOGGER.error("Swap file was not deleted. It should be deleted manually.");
                    }
                }
                runMiNiFi.setReloading(false);
            }
        } catch (InterruptedException ie) {
        }
    }

    private void initConfigFiles(Properties bootstrapProperties, String confDir) throws IOException {
        File configFile = new File(bootstrapProperties.getProperty(MINIFI_CONFIG_FILE_KEY));
        try (InputStream inputStream = new FileInputStream(configFile)) {
            ByteBuffer tempConfigFile = generateConfigFiles(inputStream, confDir, bootstrapProperties);
            runMiNiFi.getConfigFileReference().set(tempConfigFile.asReadOnlyBuffer());
        } catch (FileNotFoundException e) {
            String fileNotFoundMessage = "The config file defined in " + MINIFI_CONFIG_FILE_KEY + " does not exists.";
            DEFAULT_LOGGER.error(fileNotFoundMessage, e);
            throw new StartupFailureException(fileNotFoundMessage);
        } catch (ConfigurationChangeException e) {
            String malformedConfigFileMessage = "The config file is malformed, unable to start.";
            DEFAULT_LOGGER.error(malformedConfigFileMessage, e);
            throw new StartupFailureException(malformedConfigFileMessage);
        }
    }

    private Tuple<ProcessBuilder, Process> startMiNiFi() throws IOException {
        ProcessBuilder builder = new ProcessBuilder();

        File workingDir = getWorkingDir();
        MiNiFiListener listener = new MiNiFiListener();
        int listenPort = listener.start(runMiNiFi);
        List<String> cmd = miNiFiExecCommandProvider.getMiNiFiExecCommand(listenPort, workingDir);

        builder.command(cmd);
        builder.directory(workingDir);

        CMD_LOGGER.info("Starting Apache MiNiFi...");
        CMD_LOGGER.info("Working Directory: {}", workingDir.getAbsolutePath());
        CMD_LOGGER.info("Command: {}", cmd.stream().collect(Collectors.joining(" ")));

        return new Tuple<>(builder, startMiNiFiProcess(builder));
    }

    private Process startMiNiFiProcess(ProcessBuilder builder) throws IOException {
        Process process = builder.start();
        miNiFiStdLogHandler.initLogging(process);
        Long pid = OSUtils.getProcessId(process, CMD_LOGGER);
        if (pid != null) {
            miNiFiParameters.setMinifiPid(pid);
            Properties minifiProps = new Properties();
            minifiProps.setProperty(STATUS_FILE_PID_KEY, String.valueOf(pid));
            bootstrapFileProvider.saveStatusProperties(minifiProps);
        }

        shutdownHook = new ShutdownHook(runMiNiFi, miNiFiStdLogHandler);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        return process;
    }

    private File getWorkingDir() throws IOException {
        Properties props = bootstrapFileProvider.getBootstrapProperties();
        File bootstrapConfigAbsoluteFile = bootstrapConfigFile.getAbsoluteFile();
        File binDir = bootstrapConfigAbsoluteFile.getParentFile();

        File workingDir = Optional.ofNullable(props.getProperty("working.dir"))
            .map(File::new)
            .orElse(binDir.getParentFile());
        return workingDir;
    }

    private boolean waitForStart() {
        lock.lock();
        try {
            long startTime = System.nanoTime();

            while (miNiFiParameters.getMinifiPid() < 1 && miNiFiParameters.getMiNiFiPort() < 1) {
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
