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

import org.apache.commons.io.input.TeeInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.bootstrap.util.OSUtils;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeCoordinator;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeException;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeListener;
import org.apache.nifi.minifi.bootstrap.status.PeriodicStatusReporter;
import org.apache.nifi.minifi.bootstrap.util.ConfigTransformer;
import org.apache.nifi.minifi.commons.status.FlowStatusReport;
import org.apache.nifi.util.Tuple;
import org.apache.nifi.util.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.apache.nifi.minifi.commons.schema.common.BootstrapPropertyKeys.STATUS_REPORTER_COMPONENTS_KEY;

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
public class RunMiNiFi implements QueryableStatusAggregator, ConfigurationFileHolder {

    public static final String DEFAULT_CONFIG_FILE = "./conf/bootstrap.conf";
    public static final String DEFAULT_NIFI_PROPS_FILE = "./conf/nifi.properties";
    public static final String DEFAULT_JAVA_CMD = "java";
    public static final String DEFAULT_PID_DIR = "bin";
    public static final String DEFAULT_LOG_DIR = "./logs";


    public static final String CONF_DIR_KEY = "conf.dir";

    public static final String MINIFI_CONFIG_FILE_KEY = "nifi.minifi.config";

    public static final String GRACEFUL_SHUTDOWN_PROP = "graceful.shutdown.seconds";
    public static final String DEFAULT_GRACEFUL_SHUTDOWN_VALUE = "20";

    public static final String MINIFI_PID_DIR_PROP = "org.apache.nifi.minifi.bootstrap.config.pid.dir";

    public static final String MINIFI_PID_FILE_NAME = "minifi.pid";
    public static final String MINIFI_STATUS_FILE_NAME = "minifi.status";
    public static final String MINIFI_LOCK_FILE_NAME = "minifi.lock";

    public static final String PID_KEY = "pid";

    public static final int STARTUP_WAIT_SECONDS = 60;

    public static final String SHUTDOWN_CMD = "SHUTDOWN";
    public static final String RELOAD_CMD = "RELOAD";
    public static final String PING_CMD = "PING";
    public static final String DUMP_CMD = "DUMP";
    public static final String FLOW_STATUS_REPORT_CMD = "FLOW_STATUS_REPORT";

    private static final int UNINITIALIZED_CC_PORT = -1;

    private volatile boolean autoRestartNiFi = true;
    private volatile int ccPort = UNINITIALIZED_CC_PORT;
    private volatile long minifiPid = -1L;
    private volatile String secretKey;
    private volatile ShutdownHook shutdownHook;
    private volatile boolean nifiStarted;

    private final Lock startedLock = new ReentrantLock();
    private final Lock lock = new ReentrantLock();
    private final Condition startupCondition = lock.newCondition();
    private final File bootstrapConfigFile;

    // used for logging initial info; these will be logged to console by default when the app is started
    private final Logger cmdLogger = LoggerFactory.getLogger("org.apache.nifi.minifi.bootstrap.Command");
    // used for logging all info. These by default will be written to the log file
    private final Logger defaultLogger = LoggerFactory.getLogger(RunMiNiFi.class);


    private final ExecutorService loggingExecutor;
    private volatile Set<Future<?>> loggingFutures = new HashSet<>(2);
    private volatile int gracefulShutdownSeconds;

    private Set<PeriodicStatusReporter> periodicStatusReporters;

    private ConfigurationChangeCoordinator changeCoordinator;
    private MiNiFiConfigurationChangeListener changeListener;

    private final AtomicReference<ByteBuffer> currentConfigFileReference = new AtomicReference<>();

    @Override
    public AtomicReference<ByteBuffer> getConfigFileReference() {
        return currentConfigFileReference;
    }

    // Is set to true after the MiNiFi instance shuts down in preparation to be reloaded. Will be set to false after MiNiFi is successfully started again.
    private AtomicBoolean reloading = new AtomicBoolean(false);

    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

    public RunMiNiFi(final File bootstrapConfigFile) throws IOException {
        this.bootstrapConfigFile = bootstrapConfigFile;

        loggingExecutor = Executors.newFixedThreadPool(2, new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable runnable) {
                final Thread t = Executors.defaultThreadFactory().newThread(runnable);
                t.setDaemon(true);
                t.setName("MiNiFi logging handler");
                return t;
            }
        });
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println();
        System.out.println("java org.apache.nifi.minifi.bootstrap.RunMiNiFi <command> [options]");
        System.out.println();
        System.out.println("Valid commands include:");
        System.out.println("");
        System.out.println("Start : Start a new instance of Apache MiNiFi");
        System.out.println("Stop : Stop a running instance of Apache MiNiFi");
        System.out.println("Restart : Stop Apache MiNiFi, if it is running, and then start a new instance");
        System.out.println("Status : Determine if there is a running instance of Apache MiNiFi");
        System.out.println("Dump : Write a Thread Dump to the file specified by [options], or to the log if no file is given");
        System.out.println("Run : Start a new instance of Apache MiNiFi and monitor the Process, restarting if the instance dies");
        System.out.println("FlowStatus : Get the status of the MiNiFi flow. For usage, read the System Admin Guide 'FlowStatus Query Options' section.");
        System.out.println();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 1 || args.length > 3) {
            printUsage();
            return;
        }

        File dumpFile = null;

        final String cmd = args[0];
        if (cmd.equals("dump")) {
            if (args.length > 1) {
                dumpFile = new File(args[1]);
            } else {
                dumpFile = null;
            }
        }

        switch (cmd.toLowerCase()) {
            case "start":
            case "run":
            case "stop":
            case "status":
            case "dump":
            case "restart":
            case "env":
            case "flowstatus":
                break;
            default:
                printUsage();
                return;
        }

        final File configFile = getBootstrapConfFile();
        final RunMiNiFi runMiNiFi = new RunMiNiFi(configFile);

        Integer exitStatus = null;
        switch (cmd.toLowerCase()) {
            case "start":
                runMiNiFi.start();
                break;
            case "run":
                runMiNiFi.start();
                break;
            case "stop":
                runMiNiFi.stop();
                break;
            case "status":
                exitStatus = runMiNiFi.status();
                break;
            case "restart":
                runMiNiFi.stop();
                runMiNiFi.start();
                break;
            case "dump":
                runMiNiFi.dump(dumpFile);
                break;
            case "env":
                runMiNiFi.env();
                break;
            case "flowstatus":
                if(args.length == 2) {
                    System.out.println(runMiNiFi.statusReport(args[1]));
                } else {
                    System.out.println("The 'flowStatus' command requires an input query. See the System Admin Guide 'FlowStatus Script Query' section for complete details.");
                }
                break;
        }
        if (exitStatus != null) {
            System.exit(exitStatus);
        }
    }

    public static File getBootstrapConfFile() {
        String configFilename = System.getProperty("org.apache.nifi.minifi.bootstrap.config.file");

        if (configFilename == null) {
            final String nifiHome = System.getenv("MINIFI_HOME");
            if (nifiHome != null) {
                final File nifiHomeFile = new File(nifiHome.trim());
                final File configFile = new File(nifiHomeFile, DEFAULT_CONFIG_FILE);
                configFilename = configFile.getAbsolutePath();
            }
        }

        if (configFilename == null) {
            configFilename = DEFAULT_CONFIG_FILE;
        }

        final File configFile = new File(configFilename);
        return configFile;
    }

    private File getBootstrapFile(final Logger logger, String directory, String defaultDirectory, String fileName) throws IOException {

        final File confDir = bootstrapConfigFile.getParentFile();
        final File nifiHome = confDir.getParentFile();

        String confFileDir = System.getProperty(directory);

        final File fileDir;

        if (confFileDir != null) {
            fileDir = new File(confFileDir.trim());
        } else {
            fileDir = new File(nifiHome, defaultDirectory);
        }

        FileUtils.ensureDirectoryExistAndCanAccess(fileDir);
        final File statusFile = new File(fileDir, fileName);
        logger.debug("Status File: {}", statusFile);
        return statusFile;
    }

    File getPidFile(final Logger logger) throws IOException {
        return getBootstrapFile(logger, MINIFI_PID_DIR_PROP, DEFAULT_PID_DIR, MINIFI_PID_FILE_NAME);
    }

    File getStatusFile(final Logger logger) throws IOException {
        return getBootstrapFile(logger, MINIFI_PID_DIR_PROP, DEFAULT_PID_DIR, MINIFI_STATUS_FILE_NAME);
    }

    File getLockFile(final Logger logger) throws IOException {
        return getBootstrapFile(logger, MINIFI_PID_DIR_PROP, DEFAULT_PID_DIR, MINIFI_LOCK_FILE_NAME);
    }

    File getStatusFile() throws IOException{
        return getStatusFile(defaultLogger);
    }

    public File getReloadFile(final Logger logger) {
        final File confDir = bootstrapConfigFile.getParentFile();
        final File nifiHome = confDir.getParentFile();
        final File bin = new File(nifiHome, "bin");
        final File reloadFile = new File(bin, "minifi.reload.lock");

        logger.debug("Reload File: {}", reloadFile);
        return reloadFile;
    }

    public File getSwapFile(final Logger logger) {
        final File confDir = bootstrapConfigFile.getParentFile();
        final File swapFile = new File(confDir, "swap.yml");

        logger.debug("Swap File: {}", swapFile);
        return swapFile;
    }


    private Properties loadProperties(final Logger logger) throws IOException {
        final Properties props = new Properties();
        final File statusFile = getStatusFile(logger);
        if (statusFile == null || !statusFile.exists()) {
            logger.debug("No status file to load properties from");
            return props;
        }

        try (final FileInputStream fis = new FileInputStream(getStatusFile(logger))) {
            props.load(fis);
        }

        final Map<Object, Object> modified = new HashMap<>(props);
        modified.remove("secret.key");
        logger.debug("Properties: {}", modified);

        return props;
    }

    private synchronized void saveProperties(final Properties minifiProps, final Logger logger) throws IOException {
        final String pid = minifiProps.getProperty(PID_KEY);
        if (!StringUtils.isBlank(pid)) {
            writePidFile(pid, logger);
        }

        final File statusFile = getStatusFile(logger);
        if (statusFile.exists() && !statusFile.delete()) {
            logger.warn("Failed to delete {}", statusFile);
        }

        if (!statusFile.createNewFile()) {
            throw new IOException("Failed to create file " + statusFile);
        }

        try {
            final Set<PosixFilePermission> perms = new HashSet<>();
            perms.add(PosixFilePermission.OWNER_WRITE);
            perms.add(PosixFilePermission.OWNER_READ);
            perms.add(PosixFilePermission.GROUP_READ);
            perms.add(PosixFilePermission.OTHERS_READ);
            Files.setPosixFilePermissions(statusFile.toPath(), perms);
        } catch (final Exception e) {
            logger.warn("Failed to set permissions so that only the owner can read status file {}; "
                + "this may allows others to have access to the key needed to communicate with MiNiFi. "
                + "Permissions should be changed so that only the owner can read this file", statusFile);
        }

        try (final FileOutputStream fos = new FileOutputStream(statusFile)) {
            minifiProps.store(fos, null);
            fos.getFD().sync();
        }

        logger.debug("Saved Properties {} to {}", new Object[]{minifiProps, statusFile});
    }

    private synchronized void writePidFile(final String pid, final Logger logger) throws IOException {
        final File pidFile = getPidFile(logger);
        if (pidFile.exists() && !pidFile.delete()) {
           logger.warn("Failed to delete {}", pidFile);
        }

        if (!pidFile.createNewFile()) {
            throw new IOException("Failed to create file " + pidFile);
        }

        try {
            final Set<PosixFilePermission> perms = new HashSet<>();
            perms.add(PosixFilePermission.OWNER_READ);
            perms.add(PosixFilePermission.OWNER_WRITE);
            Files.setPosixFilePermissions(pidFile.toPath(), perms);
        } catch (final Exception e) {
            logger.warn("Failed to set permissions so that only the owner can read pid file {}; "
                    + "this may allows others to have access to the key needed to communicate with MiNiFi. "
                    + "Permissions should be changed so that only the owner can read this file", pidFile);
        }

        try (final FileOutputStream fos = new FileOutputStream(pidFile)) {
            fos.write(pid.getBytes(StandardCharsets.UTF_8));
            fos.getFD().sync();
        }

        logger.debug("Saved Pid {} to {}", new Object[]{pid, pidFile});
    }

    private boolean isPingSuccessful(final int port, final String secretKey, final Logger logger) {
        logger.debug("Pinging {}", port);

        try (final Socket socket = new Socket("localhost", port)) {
            final OutputStream out = socket.getOutputStream();
            out.write((PING_CMD + " " + secretKey + "\n").getBytes(StandardCharsets.UTF_8));
            out.flush();

            logger.debug("Sent PING command");
            socket.setSoTimeout(5000);
            final InputStream in = socket.getInputStream();
            final BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            final String response = reader.readLine();
            logger.debug("PING response: {}", response);
            out.close();
            reader.close();

            return PING_CMD.equals(response);
        } catch (final IOException ioe) {
            return false;
        }
    }

    private Integer getCurrentPort(final Logger logger) throws IOException {
        final Properties props = loadProperties(logger);
        final String portVal = props.getProperty("port");
        if (portVal == null) {
            logger.debug("No Port found in status file");
            return null;
        } else {
            logger.debug("Port defined in status file: {}", portVal);
        }

        final int port = Integer.parseInt(portVal);
        final boolean success = isPingSuccessful(port, props.getProperty("secret.key"), logger);
        if (success) {
            logger.debug("Successful PING on port {}", port);
            return port;
        }

        final String pid = props.getProperty(PID_KEY);
        logger.debug("PID in status file is {}", pid);
        if (pid != null) {
            final boolean procRunning = isProcessRunning(pid, logger);
            if (procRunning) {
                return port;
            } else {
                return null;
            }
        }

        return null;
    }

    private boolean isProcessRunning(final String pid, final Logger logger) {
        try {
            // We use the "ps" command to check if the process is still running.
            final ProcessBuilder builder = new ProcessBuilder();

            builder.command("ps", "-p", pid);
            final Process proc = builder.start();

            // Look for the pid in the output of the 'ps' command.
            boolean running = false;
            String line;
            try (final InputStream in = proc.getInputStream();
                 final Reader streamReader = new InputStreamReader(in);
                 final BufferedReader reader = new BufferedReader(streamReader)) {

                while ((line = reader.readLine()) != null) {
                    if (line.trim().startsWith(pid)) {
                        running = true;
                    }
                }
            }

            // If output of the ps command had our PID, the process is running.
            if (running) {
                logger.debug("Process with PID {} is running", pid);
            } else {
                logger.debug("Process with PID {} is not running", pid);
            }

            return running;
        } catch (final IOException ioe) {
            System.err.println("Failed to determine if Process " + pid + " is running; assuming that it is not");
            return false;
        }
    }

    private Status getStatus(final Logger logger) {
        final Properties props;
        try {
            props = loadProperties(logger);
        } catch (final IOException ioe) {
            return new Status(null, null, false, false);
        }

        if (props == null) {
            return new Status(null, null, false, false);
        }

        final String portValue = props.getProperty("port");
        final String pid = props.getProperty(PID_KEY);
        final String secretKey = props.getProperty("secret.key");

        if (portValue == null && pid == null) {
            return new Status(null, null, false, false);
        }

        Integer port = null;
        boolean pingSuccess = false;
        if (portValue != null) {
            try {
                port = Integer.parseInt(portValue);
                pingSuccess = isPingSuccessful(port, secretKey, logger);
            } catch (final NumberFormatException nfe) {
                return new Status(null, null, false, false);
            }
        }

        if (pingSuccess) {
            return new Status(port, pid, true, true);
        }

        final boolean alive = (pid != null) && isProcessRunning(pid, logger);
        return new Status(port, pid, pingSuccess, alive);
    }

    public int status() throws IOException {
        final Logger logger = cmdLogger;
        final Status status = getStatus(logger);
        if (status.isRespondingToPing()) {
            logger.info("Apache MiNiFi is currently running, listening to Bootstrap on port {}, PID={}",
                new Object[]{status.getPort(), status.getPid() == null ? "unknown" : status.getPid()});
            return 0;
        }

        if (status.isProcessRunning()) {
            logger.info("Apache MiNiFi is running at PID {} but is not responding to ping requests", status.getPid());
            return 4;
        }

        if (status.getPort() == null) {
            logger.info("Apache MiNiFi is not running");
            return 3;
        }

        if (status.getPid() == null) {
            logger.info("Apache MiNiFi is not responding to Ping requests. The process may have died or may be hung");
        } else {
            logger.info("Apache MiNiFi is not running");
        }
        return 3;
    }

    public FlowStatusReport statusReport(String statusRequest) throws IOException {
        final Logger logger = cmdLogger;
        final Status status = getStatus(logger);
        final Properties props = loadProperties(logger);

        List<String> problemsGeneratingReport = new LinkedList<>();
        if (!status.isProcessRunning()) {
            problemsGeneratingReport.add("MiNiFi process is not running");
        }

        if (!status.isRespondingToPing()) {
            problemsGeneratingReport.add("MiNiFi process is not responding to pings");
        }

        if (!problemsGeneratingReport.isEmpty()) {
            FlowStatusReport flowStatusReport = new FlowStatusReport();
            flowStatusReport.setErrorsGeneratingReport(problemsGeneratingReport);
            return flowStatusReport;
        }

        return getFlowStatusReport(statusRequest, status.getPort(), props.getProperty("secret.key"), logger);
    }

    public void env() {
        final Logger logger = cmdLogger;
        final Status status = getStatus(logger);
        if (status.getPid() == null) {
            logger.info("Apache MiNiFi is not running");
            return;
        }
        final Class<?> virtualMachineClass;
        try {
            virtualMachineClass = Class.forName("com.sun.tools.attach.VirtualMachine");
        } catch (final ClassNotFoundException cnfe) {
            logger.error("Seems tools.jar (Linux / Windows JDK) or classes.jar (Mac OS) is not available in classpath");
            return;
        }
        final Method attachMethod;
        final Method detachMethod;

        try {
            attachMethod = virtualMachineClass.getMethod("attach", String.class);
            detachMethod = virtualMachineClass.getDeclaredMethod("detach");
        } catch (final Exception e) {
            logger.error("Methods required for getting environment not available", e);
            return;
        }

        final Object virtualMachine;
        try {
            virtualMachine = attachMethod.invoke(null, status.getPid());
        } catch (final Throwable t) {
            logger.error("Problem attaching to MiNiFi", t);
            return;
        }

        try {
            final Method getSystemPropertiesMethod = virtualMachine.getClass().getMethod("getSystemProperties");

            final Properties sysProps = (Properties) getSystemPropertiesMethod.invoke(virtualMachine);
            for (Entry<Object, Object> syspropEntry : sysProps.entrySet()) {
                logger.info(syspropEntry.getKey().toString() + " = " + syspropEntry.getValue().toString());
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            try {
                detachMethod.invoke(virtualMachine);
            } catch (final Exception e) {
                logger.warn("Caught exception detaching from process", e);
            }
        }
    }

    /**
     * Writes a MiNiFi thread dump to the given file; if file is null, logs at
     * INFO level instead.
     *
     * @param dumpFile the file to write the dump content to
     * @throws IOException if any issues occur while writing the dump file
     */
    public void dump(final File dumpFile) throws IOException {
        final Logger logger = defaultLogger;    // dump to bootstrap log file by default
        final Integer port = getCurrentPort(logger);
        if (port == null) {
            logger.info("Apache MiNiFi is not currently running");
            return;
        }

        final Properties minifiProps = loadProperties(logger);
        final String secretKey = minifiProps.getProperty("secret.key");

        final StringBuilder sb = new StringBuilder();
        try (final Socket socket = new Socket()) {
            logger.debug("Connecting to MiNiFi instance");
            socket.setSoTimeout(60000);
            socket.connect(new InetSocketAddress("localhost", port));
            logger.debug("Established connection to MiNiFi instance.");
            socket.setSoTimeout(60000);

            logger.debug("Sending DUMP Command to port {}", port);
            final OutputStream out = socket.getOutputStream();
            out.write((DUMP_CMD + " " + secretKey + "\n").getBytes(StandardCharsets.UTF_8));
            out.flush();

            final InputStream in = socket.getInputStream();
            try (final BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line).append("\n");
                }
            }
        }

        final String dump = sb.toString();
        if (dumpFile == null) {
            logger.info(dump);
        } else {
            try (final FileOutputStream fos = new FileOutputStream(dumpFile)) {
                fos.write(dump.getBytes(StandardCharsets.UTF_8));
            }
            // we want to log to the console (by default) that we wrote the thread dump to the specified file
            cmdLogger.info("Successfully wrote thread dump to {}", dumpFile.getAbsolutePath());
        }
    }

    public void reload() throws IOException {
        final Logger logger = defaultLogger;
        final Integer port = getCurrentPort(logger);
        if (port == null) {
            logger.info("Apache MiNiFi is not currently running");
            return;
        }

        // indicate that a reload command is in progress
        final File reloadLockFile = getReloadFile(logger);
        if (!reloadLockFile.exists()) {
            reloadLockFile.createNewFile();
        }

        final Properties minifiProps = loadProperties(logger);
        final String secretKey = minifiProps.getProperty("secret.key");
        final String pid = minifiProps.getProperty(PID_KEY);

        try (final Socket socket = new Socket()) {
            logger.debug("Connecting to MiNiFi instance");
            socket.setSoTimeout(10000);
            socket.connect(new InetSocketAddress("localhost", port));
            logger.debug("Established connection to MiNiFi instance.");
            socket.setSoTimeout(10000);

            logger.debug("Sending RELOAD Command to port {}", port);
            final OutputStream out = socket.getOutputStream();
            out.write((RELOAD_CMD + " " + secretKey + "\n").getBytes(StandardCharsets.UTF_8));
            out.flush();
            socket.shutdownOutput();

            final InputStream in = socket.getInputStream();
            int lastChar;
            final StringBuilder sb = new StringBuilder();
            while ((lastChar = in.read()) > -1) {
                sb.append((char) lastChar);
            }
            final String response = sb.toString().trim();

            logger.debug("Received response to RELOAD command: {}", response);

            if (RELOAD_CMD.equals(response)) {
                logger.info("Apache MiNiFi has accepted the Reload Command and is reloading");

                if (pid != null) {
                    final Properties bootstrapProperties = getBootstrapProperties();

                    String gracefulShutdown = bootstrapProperties.getProperty(GRACEFUL_SHUTDOWN_PROP, DEFAULT_GRACEFUL_SHUTDOWN_VALUE);
                    int gracefulShutdownSeconds;
                    try {
                        gracefulShutdownSeconds = Integer.parseInt(gracefulShutdown);
                    } catch (final NumberFormatException nfe) {
                        gracefulShutdownSeconds = Integer.parseInt(DEFAULT_GRACEFUL_SHUTDOWN_VALUE);
                    }

                    final long startWait = System.nanoTime();
                    while (isProcessRunning(pid, logger)) {
                        logger.info("Waiting for Apache MiNiFi to finish shutting down...");
                        final long waitNanos = System.nanoTime() - startWait;
                        final long waitSeconds = TimeUnit.NANOSECONDS.toSeconds(waitNanos);
                        if (waitSeconds >= gracefulShutdownSeconds && gracefulShutdownSeconds > 0) {
                            if (isProcessRunning(pid, logger)) {
                                logger.warn("MiNiFi has not finished shutting down after {} seconds as part of configuration reload. Killing process.", gracefulShutdownSeconds);
                                try {
                                    killProcessTree(pid, logger);
                                } catch (final IOException ioe) {
                                    logger.error("Failed to kill Process with PID {}", pid);
                                }
                            }
                            break;
                        } else {
                            try {
                                Thread.sleep(2000L);
                            } catch (final InterruptedException ie) {
                            }
                        }
                    }

                    reloading.set(true);
                    logger.info("MiNiFi has finished shutting down and will be reloaded.");
                }
            } else {
                logger.error("When sending RELOAD command to MiNiFi, got unexpected response {}", response);
            }
        } catch (final IOException ioe) {
            if (pid == null) {
                logger.error("Failed to send shutdown command to port {} due to {}. No PID found for the MiNiFi process, so unable to kill process; "
                    + "the process should be killed manually.", new Object[]{port, ioe.toString()});
            } else {
                logger.error("Failed to send shutdown command to port {} due to {}. Will kill the MiNiFi Process with PID {}.", port, ioe.toString(), pid);
                killProcessTree(pid, logger);
            }
        } finally {
            if (reloadLockFile.exists() && !reloadLockFile.delete()) {
                logger.error("Failed to delete reload lock file {}; this file should be cleaned up manually", reloadLockFile);
            }
        }
    }

    public void stop() throws IOException {
        final Logger logger = cmdLogger;
        final Integer port = getCurrentPort(logger);
        if (port == null) {
            logger.info("Apache MiNiFi is not currently running");
            return;
        }

        // indicate that a stop command is in progress
        final File lockFile = getLockFile(logger);
        if (!lockFile.exists()) {
            lockFile.createNewFile();
        }

        final Properties minifiProps = loadProperties(logger);
        final String secretKey = minifiProps.getProperty("secret.key");
        final String pid = minifiProps.getProperty(PID_KEY);
        final File statusFile = getStatusFile(logger);
        final File pidFile = getPidFile(logger);

        try (final Socket socket = new Socket()) {
            logger.debug("Connecting to MiNiFi instance");
            socket.setSoTimeout(10000);
            socket.connect(new InetSocketAddress("localhost", port));
            logger.debug("Established connection to MiNiFi instance.");
            socket.setSoTimeout(10000);

            logger.debug("Sending SHUTDOWN Command to port {}", port);
            final OutputStream out = socket.getOutputStream();
            out.write((SHUTDOWN_CMD + " " + secretKey + "\n").getBytes(StandardCharsets.UTF_8));
            out.flush();
            socket.shutdownOutput();

            final InputStream in = socket.getInputStream();
            int lastChar;
            final StringBuilder sb = new StringBuilder();
            while ((lastChar = in.read()) > -1) {
                sb.append((char) lastChar);
            }
            final String response = sb.toString().trim();

            logger.debug("Received response to SHUTDOWN command: {}", response);

            if (SHUTDOWN_CMD.equals(response)) {
                logger.info("Apache MiNiFi has accepted the Shutdown Command and is shutting down now");

                if (pid != null) {
                    final Properties bootstrapProperties = getBootstrapProperties();

                    String gracefulShutdown = bootstrapProperties.getProperty(GRACEFUL_SHUTDOWN_PROP, DEFAULT_GRACEFUL_SHUTDOWN_VALUE);
                    int gracefulShutdownSeconds;
                    try {
                        gracefulShutdownSeconds = Integer.parseInt(gracefulShutdown);
                    } catch (final NumberFormatException nfe) {
                        gracefulShutdownSeconds = Integer.parseInt(DEFAULT_GRACEFUL_SHUTDOWN_VALUE);
                    }

                    final long startWait = System.nanoTime();
                    while (isProcessRunning(pid, logger)) {
                        logger.info("Waiting for Apache MiNiFi to finish shutting down...");
                        final long waitNanos = System.nanoTime() - startWait;
                        final long waitSeconds = TimeUnit.NANOSECONDS.toSeconds(waitNanos);
                        if (waitSeconds >= gracefulShutdownSeconds && gracefulShutdownSeconds > 0) {
                            if (isProcessRunning(pid, logger)) {
                                logger.warn("MiNiFi has not finished shutting down after {} seconds. Killing process.", gracefulShutdownSeconds);
                                try {
                                    killProcessTree(pid, logger);
                                } catch (final IOException ioe) {
                                    logger.error("Failed to kill Process with PID {}", pid);
                                }
                            }
                            break;
                        } else {
                            try {
                                Thread.sleep(2000L);
                            } catch (final InterruptedException ie) {
                            }
                        }
                    }

                    if (statusFile.exists() && !statusFile.delete()) {
                        logger.error("Failed to delete status file {}; this file should be cleaned up manually", statusFile);
                    }

                    if (pidFile.exists() && !pidFile.delete()) {
                        logger.error("Failed to delete pid file {}; this file should be cleaned up manually", pidFile);
                    }

                    logger.info("MiNiFi has finished shutting down.");
                }
            } else {
                logger.error("When sending SHUTDOWN command to MiNiFi, got unexpected response {}", response);
            }
        } catch (final IOException ioe) {
            if (pid == null) {
                logger.error("Failed to send shutdown command to port {} due to {}. No PID found for the MiNiFi process, so unable to kill process; "
                    + "the process should be killed manually.", new Object[]{port, ioe.toString()});
            } else {
                logger.error("Failed to send shutdown command to port {} due to {}. Will kill the MiNiFi Process with PID {}.", new Object[]{port, ioe.toString(), pid});
                killProcessTree(pid, logger);
                if (statusFile.exists() && !statusFile.delete()) {
                    logger.error("Failed to delete status file {}; this file should be cleaned up manually", statusFile);
                }
            }
        } finally {
            if (lockFile.exists() && !lockFile.delete()) {
                logger.error("Failed to delete lock file {}; this file should be cleaned up manually", lockFile);
            }
        }
    }

    private Properties getBootstrapProperties() throws IOException {
        final Properties bootstrapProperties = new Properties();
        try (final FileInputStream fis = new FileInputStream(bootstrapConfigFile)) {
            bootstrapProperties.load(fis);
        }
        return bootstrapProperties;
    }

    private static List<String> getChildProcesses(final String ppid) throws IOException {
        final Process proc = Runtime.getRuntime().exec(new String[]{"ps", "-o", PID_KEY, "--no-headers", "--ppid", ppid});
        final List<String> childPids = new ArrayList<>();
        try (final InputStream in = proc.getInputStream();
             final BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {

            String line;
            while ((line = reader.readLine()) != null) {
                childPids.add(line.trim());
            }
        }

        return childPids;
    }

    private void killProcessTree(final String pid, final Logger logger) throws IOException {
        logger.debug("Killing Process Tree for PID {}", pid);

        final List<String> children = getChildProcesses(pid);
        logger.debug("Children of PID {}: {}", new Object[]{pid, children});

        for (final String childPid : children) {
            killProcessTree(childPid, logger);
        }

        Runtime.getRuntime().exec(new String[]{"kill", "-9", pid});
    }

    public static boolean isAlive(final Process process) {
        try {
            process.exitValue();
            return false;
        } catch (final IllegalStateException | IllegalThreadStateException itse) {
            return true;
        }
    }

    private String getHostname() {
        String hostname = "Unknown Host";
        String ip = "Unknown IP Address";
        try {
            final InetAddress localhost = InetAddress.getLocalHost();
            hostname = localhost.getHostName();
            ip = localhost.getHostAddress();
        } catch (final Exception e) {
            defaultLogger.warn("Failed to obtain hostname for notification due to:", e);
        }

        return hostname + " (" + ip + ")";
    }

    private int getGracefulShutdownSeconds(Map<String, String> props, File bootstrapConfigAbsoluteFile) {
        String gracefulShutdown = props.get(GRACEFUL_SHUTDOWN_PROP);
        if (gracefulShutdown == null) {
            gracefulShutdown = DEFAULT_GRACEFUL_SHUTDOWN_VALUE;
        }

        final int gracefulShutdownSeconds;
        try {
            gracefulShutdownSeconds = Integer.parseInt(gracefulShutdown);
        } catch (final NumberFormatException nfe) {
            throw new NumberFormatException("The '" + GRACEFUL_SHUTDOWN_PROP + "' property in Bootstrap Config File "
                + bootstrapConfigAbsoluteFile.getAbsolutePath() + " has an invalid value. Must be a non-negative integer");
        }

        if (gracefulShutdownSeconds < 0) {
            throw new NumberFormatException("The '" + GRACEFUL_SHUTDOWN_PROP + "' property in Bootstrap Config File "
                + bootstrapConfigAbsoluteFile.getAbsolutePath() + " has an invalid value. Must be a non-negative integer");
        }
        return gracefulShutdownSeconds;
    }

    private Map<String, String> readProperties() throws IOException {
        if (!bootstrapConfigFile.exists()) {
            throw new FileNotFoundException(bootstrapConfigFile.getAbsolutePath());
        }

        final Properties properties = new Properties();
        try (final FileInputStream fis = new FileInputStream(bootstrapConfigFile)) {
            properties.load(fis);
        }

        final Map<String, String> props = new HashMap<>();
        props.putAll((Map) properties);
        return props;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Tuple<ProcessBuilder, Process> startMiNiFi() throws IOException, InterruptedException {
        final Integer port = getCurrentPort(cmdLogger);
        if (port != null) {
            cmdLogger.info("Apache MiNiFi is already running, listening to Bootstrap on port " + port);
            return null;
        }

        final File prevLockFile = getLockFile(cmdLogger);
        if (prevLockFile.exists() && !prevLockFile.delete()) {
            cmdLogger.warn("Failed to delete previous lock file {}; this file should be cleaned up manually", prevLockFile);
        }

        final ProcessBuilder builder = new ProcessBuilder();

        final Map<String, String> props = readProperties();

        final String specifiedWorkingDir = props.get("working.dir");
        if (specifiedWorkingDir != null) {
            builder.directory(new File(specifiedWorkingDir));
        }

        final File bootstrapConfigAbsoluteFile = bootstrapConfigFile.getAbsoluteFile();
        final File binDir = bootstrapConfigAbsoluteFile.getParentFile();
        final File workingDir = binDir.getParentFile();

        if (specifiedWorkingDir == null) {
            builder.directory(workingDir);
        }

        final String minifiLogDir = replaceNull(System.getProperty("org.apache.nifi.minifi.bootstrap.config.log.dir"), DEFAULT_LOG_DIR).trim();

        final String libFilename = replaceNull(props.get("lib.dir"), "./lib").trim();
        File libDir = getFile(libFilename, workingDir);

        final String confFilename = replaceNull(props.get(CONF_DIR_KEY), "./conf").trim();
        File confDir = getFile(confFilename, workingDir);

        String minifiPropsFilename = props.get("props.file");
        if (minifiPropsFilename == null) {
            if (confDir.exists()) {
                minifiPropsFilename = new File(confDir, "nifi.properties").getAbsolutePath();
            } else {
                minifiPropsFilename = DEFAULT_CONFIG_FILE;
            }
        }

        minifiPropsFilename = minifiPropsFilename.trim();

        final List<String> javaAdditionalArgs = new ArrayList<>();
        for (final Entry<String, String> entry : props.entrySet()) {
            final String key = entry.getKey();
            final String value = entry.getValue();

            if (key.startsWith("java.arg")) {
                javaAdditionalArgs.add(value);
            }
        }

        final File[] libFiles = libDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(final File dir, final String filename) {
                return filename.toLowerCase().endsWith(".jar");
            }
        });

        if (libFiles == null || libFiles.length == 0) {
            throw new RuntimeException("Could not find lib directory at " + libDir.getAbsolutePath());
        }

        final File[] confFiles = confDir.listFiles();
        if (confFiles == null || confFiles.length == 0) {
            throw new RuntimeException("Could not find conf directory at " + confDir.getAbsolutePath());
        }

        final List<String> cpFiles = new ArrayList<>(confFiles.length + libFiles.length);
        cpFiles.add(confDir.getAbsolutePath());
        for (final File file : libFiles) {
            cpFiles.add(file.getAbsolutePath());
        }

        final StringBuilder classPathBuilder = new StringBuilder();
        for (int i = 0; i < cpFiles.size(); i++) {
            final String filename = cpFiles.get(i);
            classPathBuilder.append(filename);
            if (i < cpFiles.size() - 1) {
                classPathBuilder.append(File.pathSeparatorChar);
            }
        }

        final String classPath = classPathBuilder.toString();
        String javaCmd = props.get("java");
        if (javaCmd == null) {
            javaCmd = DEFAULT_JAVA_CMD;
        }
        if (javaCmd.equals(DEFAULT_JAVA_CMD)) {
            String javaHome = System.getenv("JAVA_HOME");
            if (javaHome != null) {
                String fileExtension = isWindows() ? ".exe" : "";
                File javaFile = new File(javaHome + File.separatorChar + "bin"
                    + File.separatorChar + "java" + fileExtension);
                if (javaFile.exists() && javaFile.canExecute()) {
                    javaCmd = javaFile.getAbsolutePath();
                }
            }
        }

        final MiNiFiListener listener = new MiNiFiListener();
        final int listenPort = listener.start(this);

        final List<String> cmd = new ArrayList<>();

        cmd.add(javaCmd);
        cmd.add("-classpath");
        cmd.add(classPath);
        cmd.addAll(javaAdditionalArgs);
        cmd.add("-Dnifi.properties.file.path=" + minifiPropsFilename);
        cmd.add("-Dnifi.bootstrap.listen.port=" + listenPort);
        cmd.add("-Dapp=MiNiFi");
        cmd.add("-Dorg.apache.nifi.minifi.bootstrap.config.log.dir="+minifiLogDir);
        cmd.add("org.apache.nifi.minifi.MiNiFi");

        builder.command(cmd);

        final StringBuilder cmdBuilder = new StringBuilder();
        for (final String s : cmd) {
            cmdBuilder.append(s).append(" ");
        }

        cmdLogger.info("Starting Apache MiNiFi...");
        cmdLogger.info("Working Directory: {}", workingDir.getAbsolutePath());
        cmdLogger.info("Command: {}", cmdBuilder.toString());


        Process process = builder.start();
        handleLogging(process);
        Long pid = OSUtils.getProcessId(process, cmdLogger);
        if (pid != null) {
            minifiPid = pid;
            final Properties minifiProps = new Properties();
            minifiProps.setProperty(PID_KEY, String.valueOf(minifiPid));
            saveProperties(minifiProps, cmdLogger);
        }

        gracefulShutdownSeconds = getGracefulShutdownSeconds(props, bootstrapConfigAbsoluteFile);
        shutdownHook = new ShutdownHook(process, this, secretKey, gracefulShutdownSeconds, loggingExecutor);
        final Runtime runtime = Runtime.getRuntime();
        runtime.addShutdownHook(shutdownHook);

        return new Tuple<ProcessBuilder, Process>(builder, process);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void start() throws IOException, InterruptedException {

        final String confDir = getBootstrapProperties().getProperty(CONF_DIR_KEY);
        final File configFile = new File(getBootstrapProperties().getProperty(MINIFI_CONFIG_FILE_KEY));
        try (InputStream inputStream = new FileInputStream(configFile)) {
            ByteBuffer tempConfigFile = performTransformation(inputStream, confDir);
            currentConfigFileReference.set(tempConfigFile.asReadOnlyBuffer());
        } catch (ConfigurationChangeException e) {
            defaultLogger.error("The config file is malformed, unable to start.", e);
            return;
        }

        // Instantiate configuration listener and configured ingestors
        this.changeListener = new MiNiFiConfigurationChangeListener(this, defaultLogger);
        this.periodicStatusReporters = initializePeriodicNotifiers();
        startPeriodicNotifiers();
        try {
            this.changeCoordinator = initializeNotifier(this.changeListener);
        } catch (Exception e) {
            final String errorMsg = "Unable to start as {} is not properly configured due to: {}";
            cmdLogger.error(errorMsg, this.changeListener.getDescriptor(), e.getMessage());
            defaultLogger.error("Unable to initialize notifier.", e);
            // if we fail to initialize, exit without attempting to start
            System.exit(1);
        }

        Tuple<ProcessBuilder, Process> tuple = startMiNiFi();
        if (tuple == null) {
            cmdLogger.info("Start method returned null, ending start command.");
            return;
        }

        ProcessBuilder builder = tuple.getKey();
        Process process = tuple.getValue();

        try {
            while (true) {
                final boolean alive = isAlive(process);

                if (alive) {
                    try {
                        Thread.sleep(1000L);

                        if (reloading.get() && getNifiStarted()) {
                            final File swapConfigFile = getSwapFile(defaultLogger);
                            if (swapConfigFile.exists()) {
                                defaultLogger.info("MiNiFi has finished reloading successfully and swap file exists. Deleting old configuration.");

                                if (swapConfigFile.delete()) {
                                    defaultLogger.info("Swap file was successfully deleted.");
                                } else {
                                    defaultLogger.error("Swap file was not deleted. It should be deleted manually.");
                                }
                            }

                            reloading.set(false);
                        }

                    } catch (final InterruptedException ie) {
                    }
                } else {
                    final Runtime runtime = Runtime.getRuntime();
                    try {
                        runtime.removeShutdownHook(shutdownHook);
                    } catch (final IllegalStateException ise) {
                        // happens when already shutting down
                    }

                    if (autoRestartNiFi) {
                        final File statusFile = getStatusFile(defaultLogger);
                        if (!statusFile.exists()) {
                            defaultLogger.info("Status File no longer exists. Will not restart MiNiFi");
                            return;
                        }

                        final File lockFile = getLockFile(defaultLogger);
                        if (lockFile.exists()) {
                            defaultLogger.info("A shutdown was initiated. Will not restart MiNiFi");
                            return;
                        }

                        final File reloadFile = getReloadFile(defaultLogger);
                        if (reloadFile.exists()) {
                            defaultLogger.info("Currently reloading configuration. Will wait to restart MiNiFi.");
                            Thread.sleep(5000L);
                            continue;
                        }

                        final boolean previouslyStarted = getNifiStarted();
                        if (!previouslyStarted) {
                            final File swapConfigFile = getSwapFile(defaultLogger);
                            if (swapConfigFile.exists()) {
                                defaultLogger.info("Swap file exists, MiNiFi failed trying to change configuration. Reverting to old configuration.");

                                try {
                                    ByteBuffer tempConfigFile = performTransformation(new FileInputStream(swapConfigFile), confDir);
                                    currentConfigFileReference.set(tempConfigFile.asReadOnlyBuffer());
                                } catch (ConfigurationChangeException e) {
                                    defaultLogger.error("The swap file is malformed, unable to restart from prior state. Will not attempt to restart MiNiFi. Swap File should be cleaned up manually.");
                                    return;
                                }

                                Files.copy(swapConfigFile.toPath(), Paths.get(getBootstrapProperties().getProperty(MINIFI_CONFIG_FILE_KEY)), REPLACE_EXISTING);

                                defaultLogger.info("Replacing config file with swap file and deleting swap file");
                                if (!swapConfigFile.delete()) {
                                    defaultLogger.warn("The swap file failed to delete after replacing using it to revert to the old configuration. It should be cleaned up manually.");
                                }
                                reloading.set(false);
                            } else {
                                defaultLogger.info("MiNiFi either never started or failed to restart. Will not attempt to restart MiNiFi");
                                return;
                            }
                        } else {
                            setNiFiStarted(false);
                        }

                        secretKey = null;
                        process = builder.start();
                        handleLogging(process);

                        Long pid = OSUtils.getProcessId(process, defaultLogger);
                        if (pid != null) {
                            minifiPid = pid;
                            final Properties minifiProps = new Properties();
                            minifiProps.setProperty(PID_KEY, String.valueOf(minifiPid));
                            saveProperties(minifiProps, defaultLogger);
                        }

                        shutdownHook = new ShutdownHook(process, this, secretKey, gracefulShutdownSeconds, loggingExecutor);
                        runtime.addShutdownHook(shutdownHook);

                        final boolean started = waitForStart();

                        if (started) {
                            defaultLogger.info("Successfully spawned the thread to start Apache MiNiFi{}", (pid == null ? "" : " with PID " + pid));
                        } else {
                            defaultLogger.error("Apache MiNiFi does not appear to have started");
                        }
                    } else {
                        return;
                    }
                }
            }
        } finally {
            shutdownChangeNotifier();
            shutdownPeriodicStatusReporters();
        }
    }

    public FlowStatusReport getFlowStatusReport(String statusRequest, final int port, final String secretKey, final Logger logger) throws IOException {
        logger.debug("Pinging {}", port);

        try (final Socket socket = new Socket("localhost", port)) {
            final OutputStream out = socket.getOutputStream();
            final String commandWithArgs = FLOW_STATUS_REPORT_CMD + " " + secretKey +" " + statusRequest + "\n";
            out.write((commandWithArgs).getBytes(StandardCharsets.UTF_8));
            logger.debug("Sending command to MiNiFi: {}",commandWithArgs);
            out.flush();

            logger.debug("Sent FLOW_STATUS_REPORT_CMD to MiNiFi");
            socket.setSoTimeout(5000);
            final InputStream in = socket.getInputStream();

            ObjectInputStream ois = new ObjectInputStream(in);
            logger.debug("FLOW_STATUS_REPORT_CMD response received");
            Object o = ois.readObject();
            ois.close();
            out.close();
            try {
                return FlowStatusReport.class.cast(o);
            } catch (ClassCastException e) {
                String message = String.class.cast(o);
                FlowStatusReport flowStatusReport = new FlowStatusReport();
                flowStatusReport.setErrorsGeneratingReport(Collections.singletonList("Failed to get status report from MiNiFi due to:" + message));
                return flowStatusReport;
            }
        } catch (EOFException | ClassNotFoundException | SocketTimeoutException e) {
            throw new IllegalStateException("Failed to get the status report from the MiNiFi process. Potentially due to the process currently being down (restarting or otherwise).", e);
        }
    }

    private void handleLogging(final Process process) {
        final Set<Future<?>> existingFutures = loggingFutures;
        if (existingFutures != null) {
            for (final Future<?> future : existingFutures) {
                future.cancel(false);
            }
        }

        final Future<?> stdOutFuture = loggingExecutor.submit(new Runnable() {
            @Override
            public void run() {
                final Logger stdOutLogger = LoggerFactory.getLogger("org.apache.nifi.minifi.StdOut");
                final InputStream in = process.getInputStream();
                try (final BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        stdOutLogger.info(line);
                    }
                } catch (IOException e) {
                    defaultLogger.error("Failed to read from MiNiFi's Standard Out stream", e);
                }
            }
        });

        final Future<?> stdErrFuture = loggingExecutor.submit(new Runnable() {
            @Override
            public void run() {
                final Logger stdErrLogger = LoggerFactory.getLogger("org.apache.nifi.minifi.StdErr");
                final InputStream in = process.getErrorStream();
                try (final BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        stdErrLogger.error(line);
                    }
                } catch (IOException e) {
                    defaultLogger.error("Failed to read from MiNiFi's Standard Error stream", e);
                }
            }
        });

        final Set<Future<?>> futures = new HashSet<>();
        futures.add(stdOutFuture);
        futures.add(stdErrFuture);
        this.loggingFutures = futures;
    }

    private boolean isWindows() {
        final String osName = System.getProperty("os.name");
        return osName != null && osName.toLowerCase().contains("win");
    }

    private boolean waitForStart() {
        lock.lock();
        try {
            final long startTime = System.nanoTime();

            while (ccPort < 1) {
                try {
                    startupCondition.await(1, TimeUnit.SECONDS);
                } catch (final InterruptedException ie) {
                    return false;
                }

                final long waitNanos = System.nanoTime() - startTime;
                final long waitSeconds = TimeUnit.NANOSECONDS.toSeconds(waitNanos);
                if (waitSeconds > STARTUP_WAIT_SECONDS) {
                    return false;
                }
            }
        } finally {
            lock.unlock();
        }
        return true;
    }

    private File getFile(final String filename, final File workingDir) {
        File file = new File(filename);
        if (!file.isAbsolute()) {
            file = new File(workingDir, filename);
        }

        return file;
    }

    private String replaceNull(final String value, final String replacement) {
        return (value == null) ? replacement : value;
    }

    void setAutoRestartNiFi(final boolean restart) {
        this.autoRestartNiFi = restart;
    }

    void setMiNiFiCommandControlPort(final int port, final String secretKey) throws IOException {

        if (this.secretKey != null && this.ccPort != UNINITIALIZED_CC_PORT) {
            defaultLogger.warn("Blocking attempt to change MiNiFi command port and secret after they have already been initialized. requestedPort={}", port);
            return;
        }

        this.ccPort = port;
        this.secretKey = secretKey;

        if (shutdownHook != null) {
            shutdownHook.setSecretKey(secretKey);
        }

        final File statusFile = getStatusFile(defaultLogger);

        final Properties minifiProps = new Properties();
        if (minifiPid != -1) {
            minifiProps.setProperty(PID_KEY, String.valueOf(minifiPid));
        }
        minifiProps.setProperty("port", String.valueOf(ccPort));
        minifiProps.setProperty("secret.key", secretKey);

        try {
            saveProperties(minifiProps, defaultLogger);
        } catch (final IOException ioe) {
            defaultLogger.warn("Apache MiNiFi has started but failed to persist MiNiFi Port information to {} due to {}", new Object[]{statusFile.getAbsolutePath(), ioe});
        }

        defaultLogger.info("The thread to run Apache MiNiFi is now running and listening for Bootstrap requests on port {}", port);
    }

    int getNiFiCommandControlPort() {
        return this.ccPort;
    }

    void setNiFiStarted(final boolean nifiStarted) {
        startedLock.lock();
        try {
            this.nifiStarted = nifiStarted;
        } finally {
            startedLock.unlock();
        }
    }

    boolean getNifiStarted() {
        startedLock.lock();
        try {
            return nifiStarted;
        } finally {
            startedLock.unlock();
        }
    }

    public void shutdownChangeNotifier() {
        try {
            getChangeCoordinator().close();
        } catch (IOException e) {
            defaultLogger.warn("Could not successfully stop notifier ", e);
        }
    }

    public ConfigurationChangeCoordinator getChangeCoordinator() {
        return changeCoordinator;
    }

    private ConfigurationChangeCoordinator initializeNotifier(ConfigurationChangeListener configChangeListener) throws IOException {
        final Properties bootstrapProperties = getBootstrapProperties();

        ConfigurationChangeCoordinator notifier = new ConfigurationChangeCoordinator();
        notifier.initialize(bootstrapProperties, this, Collections.singleton(configChangeListener));
        notifier.start();

        return notifier;
    }

    public Set<PeriodicStatusReporter> getPeriodicStatusReporters() {
        return Collections.unmodifiableSet(periodicStatusReporters);
    }

    public void shutdownPeriodicStatusReporters() {
        for (PeriodicStatusReporter periodicStatusReporter : getPeriodicStatusReporters()) {
            try {
                periodicStatusReporter.stop();
            } catch (Exception exception) {
                System.out.println("Could not successfully stop periodic status reporter " + periodicStatusReporter.getClass() + " due to " + exception);
            }
        }
    }

    private Set<PeriodicStatusReporter> initializePeriodicNotifiers() throws IOException {
        final Set<PeriodicStatusReporter> statusReporters = new HashSet<>();

        final Properties bootstrapProperties = getBootstrapProperties();

        final String reportersCsv = bootstrapProperties.getProperty(STATUS_REPORTER_COMPONENTS_KEY);
        if (reportersCsv != null && !reportersCsv.isEmpty()) {
            for (String reporterClassname : Arrays.asList(reportersCsv.split(","))) {
                try {
                    Class<?> reporterClass = Class.forName(reporterClassname);
                    PeriodicStatusReporter reporter = (PeriodicStatusReporter) reporterClass.newInstance();
                    reporter.initialize(bootstrapProperties, this);
                    statusReporters.add(reporter);
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    throw new RuntimeException("Issue instantiating notifier " + reporterClassname, e);
                }
            }
        }
        return statusReporters;
    }

    private void startPeriodicNotifiers() throws IOException {
        for (PeriodicStatusReporter periodicStatusReporter: this.periodicStatusReporters) {
            periodicStatusReporter.start();
        }
    }

    private static class MiNiFiConfigurationChangeListener implements ConfigurationChangeListener {

        private final RunMiNiFi runner;
        private final Logger logger;
        private static final ReentrantLock handlingLock = new ReentrantLock();

        public MiNiFiConfigurationChangeListener(RunMiNiFi runner, Logger logger) {
            this.runner = runner;
            this.logger = logger;
        }

        @Override
        public void handleChange(InputStream configInputStream) throws ConfigurationChangeException {
            logger.info("Received notification of a change");

            if (!handlingLock.tryLock()) {
                throw new ConfigurationChangeException("Instance is already handling another change");
            }
            try {

                final Properties bootstrapProperties = runner.getBootstrapProperties();
                final File configFile = new File(bootstrapProperties.getProperty(MINIFI_CONFIG_FILE_KEY));

                // Store the incoming stream as a byte array to be shared among components that need it
                final ByteArrayOutputStream bufferedConfigOs = new ByteArrayOutputStream();
                byte[] copyArray = new byte[1024];
                int available = -1;
                while ((available = configInputStream.read(copyArray)) > 0) {
                    bufferedConfigOs.write(copyArray, 0, available);
                }

                // Create an input stream to use for writing a config file as well as feeding to the config transformer
                try (final ByteArrayInputStream newConfigBais = new ByteArrayInputStream(bufferedConfigOs.toByteArray())) {
                    newConfigBais.mark(-1);

                    final File swapConfigFile = runner.getSwapFile(logger);
                    logger.info("Persisting old configuration to {}", swapConfigFile.getAbsolutePath());

                    try (FileInputStream configFileInputStream = new FileInputStream(configFile)) {
                        Files.copy(configFileInputStream, swapConfigFile.toPath(), REPLACE_EXISTING);
                    }

                    try {
                        logger.info("Persisting changes to {}", configFile.getAbsolutePath());
                        saveFile(newConfigBais, configFile);
                        final String confDir = bootstrapProperties.getProperty(CONF_DIR_KEY);

                        try {
                            // Reset the input stream to provide to the transformer
                            newConfigBais.reset();

                            logger.info("Performing transformation for input and saving outputs to {}", confDir);
                            ByteBuffer tempConfigFile = runner.performTransformation(newConfigBais, confDir);
                            runner.currentConfigFileReference.set(tempConfigFile.asReadOnlyBuffer());

                            try {
                                logger.info("Reloading instance with new configuration");
                                restartInstance();
                            } catch (Exception e) {
                                logger.debug("Transformation of new config file failed after transformation into Flow.xml and nifi.properties, reverting.");
                                ByteBuffer resetConfigFile = runner.performTransformation(new FileInputStream(swapConfigFile), confDir);
                                runner.currentConfigFileReference.set(resetConfigFile.asReadOnlyBuffer());
                                throw e;
                            }
                        } catch (Exception e) {
                            logger.debug("Transformation of new config file failed after replacing original with the swap file, reverting.");
                            Files.copy(new FileInputStream(swapConfigFile), configFile.toPath(), REPLACE_EXISTING);
                            throw e;
                        }
                    } catch (Exception e) {
                        logger.debug("Transformation of new config file failed after swap file was created, deleting it.");
                        if (!swapConfigFile.delete()) {
                            logger.warn("The swap file failed to delete after a failed handling of a change. It should be cleaned up manually.");
                        }
                        throw e;
                    }
                }
            } catch (ConfigurationChangeException e){
                logger.error("Unable to carry out reloading of configuration on receipt of notification event", e);
                throw e;
            } catch (IOException ioe) {
                logger.error("Unable to carry out reloading of configuration on receipt of notification event", ioe);
                throw new ConfigurationChangeException("Unable to perform reload of received configuration change", ioe);
            } finally {
                try {
                    if (configInputStream != null) {
                        configInputStream.close() ;
                    }
                } catch (IOException e) {
                    // Quietly close
                }
                handlingLock.unlock();
            }
        }

        @Override
        public String getDescriptor() {
            return "MiNiFiConfigurationChangeListener";
        }

        private void saveFile(final InputStream configInputStream, File configFile) throws IOException {
            try {
                try (final FileOutputStream configFileOutputStream = new FileOutputStream(configFile)) {
                    byte[] copyArray = new byte[1024];
                    int available = -1;
                    while ((available = configInputStream.read(copyArray)) > 0) {
                        configFileOutputStream.write(copyArray, 0, available);
                    }
                }
            } catch (IOException ioe) {
                throw new IOException("Unable to save updated configuration to the configured config file location", ioe);
            }
        }

        private void restartInstance() throws IOException {
            try {
                runner.reload();
            } catch (IOException e) {
                throw new IOException("Unable to successfully restart MiNiFi instance after configuration change.", e);
            }
        }
    }

    private ByteBuffer performTransformation(InputStream configIs, String configDestinationPath) throws ConfigurationChangeException, IOException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             TeeInputStream teeInputStream = new TeeInputStream(configIs, byteArrayOutputStream)) {

            ConfigTransformer.transformConfigFile(
                teeInputStream,
                configDestinationPath,
                getBootstrapProperties()
            );

            return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
        } catch (ConfigurationChangeException e){
            throw e;
        } catch (Exception e) {
            throw new IOException("Unable to successfully transform the provided configuration", e);
        }
    }

    private static class Status {

        private final Integer port;
        private final String pid;

        private final Boolean respondingToPing;
        private final Boolean processRunning;

        public Status(final Integer port, final String pid, final Boolean respondingToPing, final Boolean processRunning) {
            this.port = port;
            this.pid = pid;
            this.respondingToPing = respondingToPing;
            this.processRunning = processRunning;
        }

        public String getPid() {
            return pid;
        }

        public Integer getPort() {
            return port;
        }

        public boolean isRespondingToPing() {
            return Boolean.TRUE.equals(respondingToPing);
        }

        public boolean isProcessRunning() {
            return Boolean.TRUE.equals(processRunning);
        }
    }
}
