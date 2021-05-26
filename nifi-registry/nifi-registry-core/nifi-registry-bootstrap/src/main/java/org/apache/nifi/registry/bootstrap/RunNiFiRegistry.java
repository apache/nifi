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
package org.apache.nifi.registry.bootstrap;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.bootstrap.util.OSUtils;
import org.apache.nifi.registry.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>
 * The class which bootstraps Apache NiFi Registry. This class looks for the
 * bootstrap.conf file by looking in the following places (in order):</p>
 * <ol>
 * <li>Java System Property named
 * {@code org.apache.nifi.registry.bootstrap.config.file}</li>
 * <li>${NIFI_HOME}/./conf/bootstrap.conf, where ${NIFI_REGISTRY_HOME} references an
 * environment variable {@code NIFI_REGISTRY_HOME}</li>
 * <li>./conf/bootstrap.conf, where {@code ./} represents the working
 * directory.</li>
 * </ol>
 * <p>
 * If the {@code bootstrap.conf} file cannot be found, throws a {@code FileNotFoundException}.
 */
public class RunNiFiRegistry {

    public static final String DEFAULT_CONFIG_FILE = "./conf/bootstrap.conf";
    public static final String DEFAULT_JAVA_CMD = "java";
    public static final String DEFAULT_PID_DIR = "bin";
    public static final String DEFAULT_LOG_DIR = "./logs";
    public static final String DEFAULT_DOCS_DIR = "./docs";

    public static final String GRACEFUL_SHUTDOWN_PROP = "graceful.shutdown.seconds";
    public static final String DEFAULT_GRACEFUL_SHUTDOWN_VALUE = "20";

    public static final String NIFI_REGISTRY_PID_DIR_PROP = "org.apache.nifi.registry.bootstrap.config.pid.dir";
    public static final String NIFI_REGISTRY_PID_FILE_NAME = "nifi-registry.pid";
    public static final String NIFI_REGISTRY_STATUS_FILE_NAME = "nifi-registry.status";
    public static final String NIFI_REGISTRY_LOCK_FILE_NAME = "nifi-registry.lock";
    public static final String NIFI_REGISTRY_BOOTSTRAP_SENSITIVE_KEY = "nifi.registry.bootstrap.sensitive.key";

    public static final String PID_KEY = "pid";

    public static final int STARTUP_WAIT_SECONDS = 60;

    public static final String SHUTDOWN_CMD = "SHUTDOWN";
    public static final String PING_CMD = "PING";
    public static final String DUMP_CMD = "DUMP";

    private static final int UNINITIALIZED_CC_PORT = -1;

    private volatile boolean autoRestartNiFiRegistry = true;
    private volatile int ccPort = UNINITIALIZED_CC_PORT;
    private volatile long nifiRegistryPid = -1L;
    private volatile String secretKey;
    private volatile ShutdownHook shutdownHook;
    private volatile boolean nifiRegistryStarted;

    private final Lock startedLock = new ReentrantLock();
    private final Lock lock = new ReentrantLock();
    private final Condition startupCondition = lock.newCondition();

    private final File bootstrapConfigFile;

    // used for logging initial info; these will be logged to console by default when the app is started
    private final Logger cmdLogger = LoggerFactory.getLogger("org.apache.nifi.registry.bootstrap.Command");
    // used for logging all info. These by default will be written to the log file
    private final Logger defaultLogger = LoggerFactory.getLogger(RunNiFiRegistry.class);


    private final ExecutorService loggingExecutor;
    private volatile Set<Future<?>> loggingFutures = new HashSet<>(2);

    public RunNiFiRegistry(final File bootstrapConfigFile, final boolean verbose) throws IOException {
        this.bootstrapConfigFile = bootstrapConfigFile;

        loggingExecutor = Executors.newFixedThreadPool(2, new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable runnable) {
                final Thread t = Executors.defaultThreadFactory().newThread(runnable);
                t.setDaemon(true);
                t.setName("NiFi logging handler");
                return t;
            }
        });
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println();
        System.out.println("java org.apache.nifi.bootstrap.RunNiFiRegistry [<-verbose>] <command> [options]");
        System.out.println();
        System.out.println("Valid commands include:");
        System.out.println("");
        System.out.println("Start : Start a new instance of Apache NiFi Registry");
        System.out.println("Stop : Stop a running instance of Apache NiFi Registry");
        System.out.println("Restart : Stop Apache NiFi Registry, if it is running, and then start a new instance");
        System.out.println("Status : Determine if there is a running instance of Apache NiFi Registry");
        System.out.println("Dump : Write a Thread Dump to the file specified by [options], or to the log if no file is given");
        System.out.println("Run : Start a new instance of Apache NiFi Registry and monitor the Process, restarting if the instance dies");
        System.out.println();
    }

    private static String[] shift(final String[] orig) {
        return Arrays.copyOfRange(orig, 1, orig.length);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 1 || args.length > 3) {
            printUsage();
            return;
        }

        File dumpFile = null;
        boolean verbose = false;
        if (args[0].equals("-verbose")) {
            verbose = true;
            args = shift(args);
        }

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
                break;
            default:
                printUsage();
                return;
        }

        final File configFile = getDefaultBootstrapConfFile();
        final RunNiFiRegistry runNiFiRegistry = new RunNiFiRegistry(configFile, verbose);

        Integer exitStatus = null;
        switch (cmd.toLowerCase()) {
            case "start":
                runNiFiRegistry.start();
                break;
            case "run":
                runNiFiRegistry.start();
                break;
            case "stop":
                runNiFiRegistry.stop();
                break;
            case "status":
                exitStatus = runNiFiRegistry.status();
                break;
            case "restart":
                runNiFiRegistry.stop();
                runNiFiRegistry.start();
                break;
            case "dump":
                runNiFiRegistry.dump(dumpFile);
                break;
            case "env":
                runNiFiRegistry.env();
                break;
        }
        if (exitStatus != null) {
            System.exit(exitStatus);
        }
    }

    private static File getDefaultBootstrapConfFile() {
        String configFilename = System.getProperty("org.apache.nifi.registry.bootstrap.config.file");

        if (configFilename == null) {
            final String nifiRegistryHome = System.getenv("NIFI_REGISTRY_HOME");
            if (nifiRegistryHome != null) {
                final File nifiRegistryHomeFile = new File(nifiRegistryHome.trim());
                final File configFile = new File(nifiRegistryHomeFile, DEFAULT_CONFIG_FILE);
                configFilename = configFile.getAbsolutePath();
            }
        }

        if (configFilename == null) {
            configFilename = DEFAULT_CONFIG_FILE;
        }

        final File configFile = new File(configFilename);
        return configFile;
    }

    protected File getBootstrapFile(final Logger logger, String directory, String defaultDirectory, String fileName) throws IOException {

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

    protected File getPidFile(final Logger logger) throws IOException {
        return getBootstrapFile(logger, NIFI_REGISTRY_PID_DIR_PROP, DEFAULT_PID_DIR, NIFI_REGISTRY_PID_FILE_NAME);
    }

    protected File getStatusFile(final Logger logger) throws IOException {
        return getBootstrapFile(logger, NIFI_REGISTRY_PID_DIR_PROP, DEFAULT_PID_DIR, NIFI_REGISTRY_STATUS_FILE_NAME);
    }

    protected File getLockFile(final Logger logger) throws IOException {
        return getBootstrapFile(logger, NIFI_REGISTRY_PID_DIR_PROP, DEFAULT_PID_DIR, NIFI_REGISTRY_LOCK_FILE_NAME);
    }

    protected File getStatusFile() throws IOException {
        return getStatusFile(defaultLogger);
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

    private synchronized void savePidProperties(final Properties pidProperties, final Logger logger) throws IOException {
        final String pid = pidProperties.getProperty(PID_KEY);
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
            perms.add(PosixFilePermission.OWNER_READ);
            perms.add(PosixFilePermission.OWNER_WRITE);
            Files.setPosixFilePermissions(statusFile.toPath(), perms);
        } catch (final Exception e) {
            logger.warn("Failed to set permissions so that only the owner can read status file {}; "
                    + "this may allows others to have access to the key needed to communicate with NiFi Registry. "
                    + "Permissions should be changed so that only the owner can read this file", statusFile);
        }

        try (final FileOutputStream fos = new FileOutputStream(statusFile)) {
            pidProperties.store(fos, null);
            fos.getFD().sync();
        }

        logger.debug("Saved Properties {} to {}", new Object[]{pidProperties, statusFile});
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
            perms.add(PosixFilePermission.OWNER_WRITE);
            perms.add(PosixFilePermission.OWNER_READ);
            perms.add(PosixFilePermission.GROUP_READ);
            perms.add(PosixFilePermission.OTHERS_READ);
            Files.setPosixFilePermissions(pidFile.toPath(), perms);
        } catch (final Exception e) {
            logger.warn("Failed to set permissions so that only the owner can read pid file {}; "
                    + "this may allows others to have access to the key needed to communicate with NiFi Registry. "
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

        final boolean alive = pid != null && isProcessRunning(pid, logger);
        return new Status(port, pid, pingSuccess, alive);
    }

    public int status() throws IOException {
        final Logger logger = cmdLogger;
        final Status status = getStatus(logger);
        if (status.isRespondingToPing()) {
            logger.info("Apache NiFi Registry is currently running, listening to Bootstrap on port {}, PID={}",
                    new Object[]{status.getPort(), status.getPid() == null ? "unknown" : status.getPid()});
            return 0;
        }

        if (status.isProcessRunning()) {
            logger.info("Apache NiFi Registry is running at PID {} but is not responding to ping requests", status.getPid());
            return 4;
        }

        if (status.getPort() == null) {
            logger.info("Apache NiFi Registry is not running");
            return 3;
        }

        if (status.getPid() == null) {
            logger.info("Apache NiFi Registry is not responding to Ping requests. The process may have died or may be hung");
        } else {
            logger.info("Apache NiFi Registry is not running");
        }
        return 3;
    }

    public void env() {
        final Logger logger = cmdLogger;
        final Status status = getStatus(logger);
        if (status.getPid() == null) {
            logger.info("Apache NiFi Registry is not running");
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
            logger.error("Problem attaching to NiFi", t);
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
     * Writes a NiFi thread dump to the given file; if file is null, logs at
     * INFO level instead.
     *
     * @param dumpFile the file to write the dump content to
     * @throws IOException if any issues occur while writing the dump file
     */
    public void dump(final File dumpFile) throws IOException {
        final Logger logger = defaultLogger;    // dump to bootstrap log file by default
        final Integer port = getCurrentPort(logger);
        if (port == null) {
            logger.info("Apache NiFi Registry is not currently running");
            return;
        }

        final Properties nifiRegistryProps = loadProperties(logger);
        final String secretKey = nifiRegistryProps.getProperty("secret.key");

        final StringBuilder sb = new StringBuilder();
        try (final Socket socket = new Socket()) {
            logger.debug("Connecting to NiFi Registry instance");
            socket.setSoTimeout(60000);
            socket.connect(new InetSocketAddress("localhost", port));
            logger.debug("Established connection to NiFi Registry instance.");
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

    public void notifyStop() {
        final String hostname = getHostname();
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        final String now = sdf.format(System.currentTimeMillis());
        String user = System.getProperty("user.name");
        if (user == null || user.trim().isEmpty()) {
            user = "Unknown User";
        }
    }

    public void stop() throws IOException {
        final Logger logger = cmdLogger;
        final Integer port = getCurrentPort(logger);
        if (port == null) {
            logger.info("Apache NiFi Registry is not currently running");
            return;
        }

        // indicate that a stop command is in progress
        final File lockFile = getLockFile(logger);
        if (!lockFile.exists()) {
            lockFile.createNewFile();
        }

        final Properties nifiRegistryProps = loadProperties(logger);
        final String secretKey = nifiRegistryProps.getProperty("secret.key");
        final String pid = nifiRegistryProps.getProperty(PID_KEY);
        final File statusFile = getStatusFile(logger);
        final File pidFile = getPidFile(logger);

        try (final Socket socket = new Socket()) {
            logger.debug("Connecting to NiFi Registry instance");
            socket.setSoTimeout(10000);
            socket.connect(new InetSocketAddress("localhost", port));
            logger.debug("Established connection to NiFi Registry instance.");
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
                logger.info("Apache NiFi Registry has accepted the Shutdown Command and is shutting down now");

                if (pid != null) {
                    final Properties bootstrapProperties = new Properties();
                    try (final FileInputStream fis = new FileInputStream(bootstrapConfigFile)) {
                        bootstrapProperties.load(fis);
                    }

                    String gracefulShutdown = bootstrapProperties.getProperty(GRACEFUL_SHUTDOWN_PROP, DEFAULT_GRACEFUL_SHUTDOWN_VALUE);
                    int gracefulShutdownSeconds;
                    try {
                        gracefulShutdownSeconds = Integer.parseInt(gracefulShutdown);
                    } catch (final NumberFormatException nfe) {
                        gracefulShutdownSeconds = Integer.parseInt(DEFAULT_GRACEFUL_SHUTDOWN_VALUE);
                    }

                    notifyStop();
                    final long startWait = System.nanoTime();
                    while (isProcessRunning(pid, logger)) {
                        logger.info("Waiting for Apache NiFi Registry to finish shutting down...");
                        final long waitNanos = System.nanoTime() - startWait;
                        final long waitSeconds = TimeUnit.NANOSECONDS.toSeconds(waitNanos);
                        if (waitSeconds >= gracefulShutdownSeconds && gracefulShutdownSeconds > 0) {
                            if (isProcessRunning(pid, logger)) {
                                logger.warn("NiFi Registry has not finished shutting down after {} seconds. Killing process.", gracefulShutdownSeconds);
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

                    logger.info("NiFi Registry has finished shutting down.");
                }
            } else {
                logger.error("When sending SHUTDOWN command to NiFi Registry , got unexpected response {}", response);
            }
        } catch (final IOException ioe) {
            if (pid == null) {
                logger.error("Failed to send shutdown command to port {} due to {}. No PID found for the NiFi Registry process, so unable to kill process; "
                        + "the process should be killed manually.", new Object[]{port, ioe.toString()});
            } else {
                logger.error("Failed to send shutdown command to port {} due to {}. Will kill the NiFi Registry Process with PID {}.", port, ioe.toString(), pid);
                notifyStop();
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

    private static List<String> getChildProcesses(final String ppid) throws IOException {
        final Process proc = Runtime.getRuntime().exec(new String[]{"ps", "-o", "pid", "--no-headers", "--ppid", ppid});
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

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void start() throws IOException, InterruptedException {
        final Integer port = getCurrentPort(cmdLogger);
        if (port != null) {
            cmdLogger.info("Apache NiFi Registry is already running, listening to Bootstrap on port " + port);
            return;
        }

        final File prevLockFile = getLockFile(cmdLogger);
        if (prevLockFile.exists() && !prevLockFile.delete()) {
            cmdLogger.warn("Failed to delete previous lock file {}; this file should be cleaned up manually", prevLockFile);
        }

        final ProcessBuilder builder = new ProcessBuilder();

        if (!bootstrapConfigFile.exists()) {
            throw new FileNotFoundException(bootstrapConfigFile.getAbsolutePath());
        }

        final Properties properties = new Properties();
        try (final FileInputStream fis = new FileInputStream(bootstrapConfigFile)) {
            properties.load(fis);
        }

        final Map<String, String> props = new HashMap<>();
        props.putAll((Map) properties);

        // Determine the working dir for launching the NiFi Registry process
        // The order of precedence is:
        // 1) Specified in bootstrap.conf via working.dir
        // 2) NIFI_REGISTRY_HOME env variable
        // 3) Parent of bootstrap config file's parent

        final File workingDir;
        final String specifiedWorkingDir = props.get("working.dir");
        final String nifiRegistryHome = System.getenv("NIFI_REGISTRY_HOME");
        final File bootstrapConfigAbsoluteFile = bootstrapConfigFile.getAbsoluteFile();

        if (!StringUtils.isBlank(specifiedWorkingDir)) {
            workingDir = new File(specifiedWorkingDir.trim());
        } else if (!StringUtils.isBlank(nifiRegistryHome)) {
            workingDir = new File(nifiRegistryHome.trim());
        } else {
            final File binDir = bootstrapConfigAbsoluteFile.getParentFile();
            workingDir = binDir.getParentFile();
        }

        builder.directory(workingDir);

        final String nifiRegistryLogDir = replaceNull(System.getProperty("org.apache.nifi.registry.bootstrap.config.log.dir"), DEFAULT_LOG_DIR).trim();

        final String nifiRegistryDocsDir = replaceNull(props.get("docs.dir"), DEFAULT_DOCS_DIR).trim();

        final String libFilename = replaceNull(props.get("lib.dir"), "./lib").trim();
        File libDir = getFile(libFilename, workingDir);
        File libSharedDir = getFile(libFilename + "/shared", workingDir);

        final String confFilename = replaceNull(props.get("conf.dir"), "./conf").trim();
        File confDir = getFile(confFilename, workingDir);

        String nifiRegistryPropsFilename = props.get("props.file");
        if (nifiRegistryPropsFilename == null) {
            if (confDir.exists()) {
                nifiRegistryPropsFilename = new File(confDir, "nifi-registry.properties").getAbsolutePath();
            } else {
                nifiRegistryPropsFilename = DEFAULT_CONFIG_FILE;
            }
        }

        nifiRegistryPropsFilename = nifiRegistryPropsFilename.trim();

        final List<String> javaAdditionalArgs = new ArrayList<>();
        for (final Map.Entry<String, String> entry : props.entrySet()) {
            final String key = entry.getKey();
            final String value = entry.getValue();

            if (key.startsWith("java.arg")) {
                javaAdditionalArgs.add(value);
            }
        }

        final File[] libSharedFiles = libSharedDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(final File dir, final String filename) {
                return filename.toLowerCase().endsWith(".jar");
            }
        });

        if (libSharedFiles == null || libSharedFiles.length == 0) {
            throw new RuntimeException("Could not find lib shared directory at " + libSharedDir.getAbsolutePath());
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

        final List<String> cpFiles = new ArrayList<>(confFiles.length + libFiles.length + libSharedFiles.length);
        cpFiles.add(confDir.getAbsolutePath());
        for (final File file : libSharedFiles) {
            cpFiles.add(file.getAbsolutePath());
        }
        for (final File file : libFiles) {
            cpFiles.add(file.getAbsolutePath());
        }

        final String runtimeJavaVersion = System.getProperty("java.version");
        defaultLogger.info("Runtime Java version: {}", runtimeJavaVersion);
        if (Integer.parseInt(runtimeJavaVersion.substring(0, runtimeJavaVersion.indexOf('.'))) >= 11) {
            // If running on Java 11 or greater, add lib/java11 to the classpath.
            // TODO: Once the minimum Java version requirement of NiFi Registry is 11, this processing should be removed.
            final String libJava11Filename = replaceNull(props.get("lib.dir"), "./lib").trim() + "/java11";
            final File libJava11Dir = getFile(libJava11Filename, workingDir);
            if (libJava11Dir.exists()) {
                for (final File file : Objects.requireNonNull(libJava11Dir.listFiles((dir, filename) -> filename.toLowerCase().endsWith(".jar")))) {
                    cpFiles.add(file.getAbsolutePath());
                }
            }
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

        final NiFiRegistryListener listener = new NiFiRegistryListener();
        final int listenPort = listener.start(this);

        final List<String> cmd = new ArrayList<>();

        cmd.add(javaCmd);
        cmd.add("-classpath");
        cmd.add(classPath);
        cmd.addAll(javaAdditionalArgs);
        cmd.add("-Dnifi.registry.properties.file.path=" + nifiRegistryPropsFilename);
        cmd.add("-Dnifi.registry.bootstrap.config.file.path=" + bootstrapConfigFile.getAbsolutePath());
        cmd.add("-Dnifi.registry.bootstrap.listen.port=" + listenPort);
        cmd.add("-Dnifi.registry.bootstrap.config.docs.dir=" + nifiRegistryDocsDir);
        cmd.add("-Dapp=NiFiRegistry");
        cmd.add("-Dorg.apache.nifi.registry.bootstrap.config.log.dir=" + nifiRegistryLogDir);

        if (runtimeJavaVersion.startsWith("9") || runtimeJavaVersion.startsWith("10")) {
            // running on Java 9+, java.xml.bind module must be made available
            // running on Java 9 or 10, internal module java.xml.bind module must be made available
            cmd.add("--add-modules=java.xml.bind");
        }

        cmd.add("org.apache.nifi.registry.NiFiRegistry");

        builder.command(cmd);

        final StringBuilder cmdBuilder = new StringBuilder();
        for (final String s : cmd) {
            cmdBuilder.append(s).append(" ");
        }

        cmdLogger.info("Starting Apache NiFi Registry...");
        cmdLogger.info("Working Directory: {}", workingDir.getAbsolutePath());
        cmdLogger.info("Command: {}", cmdBuilder.toString());

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

        Process process = builder.start();
        handleLogging(process);
        Long pid = OSUtils.getProcessId(process, cmdLogger);
        if (pid == null) {
            cmdLogger.warn("Launched Apache NiFi Registry but could not determined the Process ID");
        } else {
            nifiRegistryPid = pid;
            final Properties pidProperties = new Properties();
            pidProperties.setProperty(PID_KEY, String.valueOf(nifiRegistryPid));
            savePidProperties(pidProperties, cmdLogger);
            cmdLogger.info("Launched Apache NiFi Registry with Process ID " + pid);
        }

        shutdownHook = new ShutdownHook(process, this, secretKey, gracefulShutdownSeconds, loggingExecutor);
        final Runtime runtime = Runtime.getRuntime();
        runtime.addShutdownHook(shutdownHook);

        final String hostname = getHostname();
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        String now = sdf.format(System.currentTimeMillis());
        String user = System.getProperty("user.name");
        if (user == null || user.trim().isEmpty()) {
            user = "Unknown User";
        }

        while (true) {
            final boolean alive = isAlive(process);

            if (alive) {
                try {
                    Thread.sleep(1000L);
                } catch (final InterruptedException ie) {
                }
            } else {
                try {
                    runtime.removeShutdownHook(shutdownHook);
                } catch (final IllegalStateException ise) {
                    // happens when already shutting down
                }

                now = sdf.format(System.currentTimeMillis());
                if (autoRestartNiFiRegistry) {
                    final File statusFile = getStatusFile(defaultLogger);
                    if (!statusFile.exists()) {
                        defaultLogger.info("Status File no longer exists. Will not restart NiFi Registry ");
                        return;
                    }

                    final File lockFile = getLockFile(defaultLogger);
                    if (lockFile.exists()) {
                        defaultLogger.info("A shutdown was initiated. Will not restart NiFi Registry ");
                        return;
                    }

                    final boolean previouslyStarted = getNifiRegistryStarted();
                    if (!previouslyStarted) {
                        defaultLogger.info("NiFi Registry never started. Will not restart NiFi Registry ");
                        return;
                    } else {
                        setNiFiRegistryStarted(false);
                    }

                    defaultLogger.warn("Apache NiFi Registry appears to have died. Restarting...");
                    secretKey = null;
                    process = builder.start();
                    handleLogging(process);

                    pid = OSUtils.getProcessId(process, defaultLogger);
                    if (pid == null) {
                        cmdLogger.warn("Launched Apache NiFi Registry but could not obtain the Process ID");
                    } else {
                        nifiRegistryPid = pid;
                        final Properties pidProperties = new Properties();
                        pidProperties.setProperty(PID_KEY, String.valueOf(nifiRegistryPid));
                        savePidProperties(pidProperties, defaultLogger);
                        cmdLogger.info("Launched Apache NiFi Registry with Process ID " + pid);
                    }

                    shutdownHook = new ShutdownHook(process, this, secretKey, gracefulShutdownSeconds, loggingExecutor);
                    runtime.addShutdownHook(shutdownHook);

                    final boolean started = waitForStart();

                    if (started) {
                        defaultLogger.info("Successfully started Apache NiFi Registry {}", (pid == null ? "" : " with PID " + pid));
                    } else {
                        defaultLogger.error("Apache NiFi Registry does not appear to have started");
                    }
                } else {
                    return;
                }
            }
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
                final Logger stdOutLogger = LoggerFactory.getLogger("org.apache.nifi.registry.StdOut");
                final InputStream in = process.getInputStream();
                try (final BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        stdOutLogger.info(line);
                    }
                } catch (IOException e) {
                    defaultLogger.error("Failed to read from NiFi Registry's Standard Out stream", e);
                }
            }
        });

        final Future<?> stdErrFuture = loggingExecutor.submit(new Runnable() {
            @Override
            public void run() {
                final Logger stdErrLogger = LoggerFactory.getLogger("org.apache.nifi.registry.StdErr");
                final InputStream in = process.getErrorStream();
                try (final BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        stdErrLogger.error(line);
                    }
                } catch (IOException e) {
                    defaultLogger.error("Failed to read from NiFi Registry's Standard Error stream", e);
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

    void setAutoRestartNiFiRegistry(final boolean restart) {
        this.autoRestartNiFiRegistry = restart;
    }

    void setNiFiRegistryCommandControlPort(final int port, final String secretKey) throws IOException {

        if (this.secretKey != null && this.ccPort != UNINITIALIZED_CC_PORT) {
            defaultLogger.warn("Blocking attempt to change NiFi Registry command port and secret after they have already been initialized. requestedPort={}", port);
            return;
        }

        this.ccPort = port;
        this.secretKey = secretKey;

        if (shutdownHook != null) {
            shutdownHook.setSecretKey(secretKey);
        }

        final File statusFile = getStatusFile(defaultLogger);

        final Properties nifiProps = new Properties();
        if (nifiRegistryPid != -1) {
            nifiProps.setProperty(PID_KEY, String.valueOf(nifiRegistryPid));
        }
        nifiProps.setProperty("port", String.valueOf(ccPort));
        nifiProps.setProperty("secret.key", secretKey);

        try {
            savePidProperties(nifiProps, defaultLogger);
        } catch (final IOException ioe) {
            defaultLogger.warn("Apache NiFi Registry has started but failed to persist NiFi Registry Port information to {} due to {}", new Object[]{statusFile.getAbsolutePath(), ioe});
        }

        defaultLogger.info("Apache NiFi Registry now running and listening for Bootstrap requests on port {}", port);
    }

    int getNiFiRegistryCommandControlPort() {
        return this.ccPort;
    }

    void setNiFiRegistryStarted(final boolean nifiStarted) {
        startedLock.lock();
        try {
            this.nifiRegistryStarted = nifiStarted;
        } finally {
            startedLock.unlock();
        }
    }

    boolean getNifiRegistryStarted() {
        startedLock.lock();
        try {
            return nifiRegistryStarted;
        } finally {
            startedLock.unlock();
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
