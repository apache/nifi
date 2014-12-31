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
package org.apache.nifi.bootstrap;

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
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;


/**
 * Bootstrap class to run Apache NiFi.
 * 
 * This class looks for the bootstrap.conf file by looking in the following places (in order):
 * <ol>
 *  <li>Java System Property named {@code org.apache.nifi.bootstrap.config.file}</li>
 *  <li>${NIFI_HOME}/./conf/bootstrap.conf, where ${NIFI_HOME} references an environment variable {@code NIFI_HOME}</li>
 *  <li>./conf/bootstrap.conf, where {@code .} represents the working directory.
 * </ol>
 *
 * If the {@code bootstrap.conf} file cannot be found, throws a {@code FileNotFoundException].
 */
public class RunNiFi {
	public static final String DEFAULT_CONFIG_FILE = "./conf/bootstrap.conf";
	public static final String DEFAULT_NIFI_PROPS_FILE = "./conf/nifi.properties";

	public static final String GRACEFUL_SHUTDOWN_PROP = "graceful.shutdown.seconds";
	public static final String DEFAULT_GRACEFUL_SHUTDOWN_VALUE = "20";
	
	public static final String RUN_AS_PROP = "run.as";
	
	public static final int MAX_RESTART_ATTEMPTS = 5;
	public static final int STARTUP_WAIT_SECONDS = 60;
	
	public static final String SHUTDOWN_CMD = "SHUTDOWN";
	public static final String PING_CMD = "PING";
	
	private volatile boolean autoRestartNiFi = true;
	private volatile int ccPort = -1;
	private volatile long nifiPid = -1L;
	private volatile String secretKey;
	private volatile ShutdownHook shutdownHook;
	
	private final Lock lock = new ReentrantLock();
	private final Condition startupCondition = lock.newCondition();
	
	private final File bootstrapConfigFile;

	private final java.util.logging.Logger logger;
	
	public RunNiFi(final File bootstrapConfigFile, final boolean verbose) {
		this.bootstrapConfigFile = bootstrapConfigFile;
		logger = java.util.logging.Logger.getLogger("Bootstrap");
		if ( verbose ) {
		    logger.info("Enabling Verbose Output");
		    
		    logger.setLevel(Level.FINE);
		    final Handler handler = new ConsoleHandler();
		    handler.setLevel(Level.FINE);
		    logger.addHandler(handler);
		}
	}
	
	private static void printUsage() {
		System.out.println("Usage:");
		System.out.println();
		System.out.println("java org.apache.nifi.bootstrap.RunNiFi [<-verbose>] <command>");
		System.out.println();
		System.out.println("Valid commands include:");
		System.out.println("");
		System.out.println("Start : Start a new instance of Apache NiFi");
		System.out.println("Stop : Stop a running instance of Apache NiFi");
		System.out.println("Status : Determine if there is a running instance of Apache NiFi");
		System.out.println("Run : Start a new instance of Apache NiFi and monitor the Process, restarting if the instance dies");
		System.out.println();
	}

	
	public static void main(final String[] args) throws IOException, InterruptedException {
		if ( args.length < 1 || args.length > 2 ) {
			printUsage();
			return;
		}
		
		boolean verbose = false;
		if ( args.length == 2 ) {
		    if ( args[0].equals("-verbose") ) {
		        verbose = true;
		    } else {
		        printUsage();
		        return;
		    }
		}
		
		final String cmd = args.length == 1 ? args[0] : args[1];
		
		switch (cmd.toLowerCase()) {
			case "start":
			case "run":
			case "stop":
			case "status":
				break;
			default:
				printUsage();
				return;
		}
		
		String configFilename = System.getProperty("org.apache.nifi.bootstrap.config.file");
		
		if ( configFilename == null ) {
			final String nifiHome = System.getenv("NIFI_HOME");
			if ( nifiHome != null ) {
				final File nifiHomeFile = new File(nifiHome.trim());
				final File configFile = new File(nifiHomeFile, DEFAULT_CONFIG_FILE);
				configFilename = configFile.getAbsolutePath();
			}
		}
		
		if ( configFilename == null ) {
			configFilename = DEFAULT_CONFIG_FILE;
		}
		
		final File configFile = new File(configFilename);
		
		final RunNiFi runNiFi = new RunNiFi(configFile, verbose);
		
		switch (cmd.toLowerCase()) {
			case "start":
				runNiFi.start(false);
				break;
			case "run":
				runNiFi.start(true);
				break;
			case "stop":
				runNiFi.stop();
				break;
			case "status":
				runNiFi.status();
				break;
		}
	}
	
	
	public File getStatusFile() {
		final File confDir = bootstrapConfigFile.getParentFile();
		final File nifiHome = confDir.getParentFile();
		final File bin = new File(nifiHome, "bin");
		final File statusFile = new File(bin, "nifi.pid");
		
		logger.fine("Status File: " + statusFile);
		
		return statusFile;
	}
	
	private Properties loadProperties() throws IOException {
	    final Properties props = new Properties();
	    final File statusFile = getStatusFile();
	    if ( statusFile == null || !statusFile.exists() ) {
	        logger.fine("No status file to load properties from");
	        return props;
	    }
	    
	    try (final FileInputStream fis = new FileInputStream(getStatusFile())) {
	        props.load(fis);
	    }
	    
	    logger.fine("Properties: " + props);
	    return props;
	}
	
	private synchronized void saveProperties(final Properties nifiProps) throws IOException {
	    final File statusFile = getStatusFile();
	    if ( statusFile.exists() && !statusFile.delete() ) {
	        logger.warning("Failed to delete " + statusFile);
	    }

	    if ( !statusFile.createNewFile() ) {
	        throw new IOException("Failed to create file " + statusFile);
	    }

	    try {
	        final Set<PosixFilePermission> perms = new HashSet<>();
	        perms.add(PosixFilePermission.OWNER_READ);
	        perms.add(PosixFilePermission.OWNER_WRITE);
	        Files.setPosixFilePermissions(statusFile.toPath(), perms);
	    } catch (final Exception e) {
	        logger.warning("Failed to set permissions so that only the owner can read status file " + statusFile + "; this may allows others to have access to the key needed to communicate with NiFi. Permissions should be changed so that only the owner can read this file");
	    }
	    
        try (final FileOutputStream fos = new FileOutputStream(statusFile)) {
            nifiProps.store(fos, null);
            fos.getFD().sync();
        }
        
        logger.fine("Saved Properties " + nifiProps + " to " + statusFile);
	}

	private boolean isPingSuccessful(final int port, final String secretKey) {
	    logger.fine("Pinging " + port);
	    
	    try (final Socket socket = new Socket("localhost", port)) {
            final OutputStream out = socket.getOutputStream();
            out.write((PING_CMD + " " + secretKey + "\n").getBytes(StandardCharsets.UTF_8));
            out.flush();

            logger.fine("Sent PING command");
            socket.setSoTimeout(5000);
            final InputStream in = socket.getInputStream();
            final BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            final String response = reader.readLine();
            logger.fine("PING response: " + response);
            
            return PING_CMD.equals(response);
	    } catch (final IOException ioe) {
	        return false;
	    }
	}
	
	private Integer getCurrentPort() throws IOException {
		final Properties props = loadProperties();
		final String portVal = props.getProperty("port");
		if ( portVal == null ) {
		    logger.fine("No Port found in status file");
		    return null;
		} else { 
		    logger.fine("Port defined in status file: " + portVal);
		}
		
		final int port = Integer.parseInt(portVal);
	    final boolean success = isPingSuccessful(port, props.getProperty("secret.key"));
	    if ( success ) {
	        logger.fine("Successful PING on port " + port);
	        return port;
	    }

	    final String pid = props.getProperty("pid");
	    logger.fine("PID in status file is " + pid);
	    if ( pid != null ) {
	        final boolean procRunning = isProcessRunning(pid);
	        if ( procRunning ) {
	            return port;
	        } else {
	            return null;
	        }
	    }
	    
	    return null;
	}
	
	
	private boolean isProcessRunning(final String pid) {
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
                    if ( line.trim().startsWith(pid) ) {
                        running = true;
                    }
	            }
	        }
	        
	        // If output of the ps command had our PID, the process is running.
	        if ( running ) {
	            logger.fine("Process with PID " + pid + " is running");
	        } else {
	            logger.fine("Process with PID " + pid + " is not running");
	        }
	        
	        return running;
	    } catch (final IOException ioe) {
	        System.err.println("Failed to determine if Process " + pid + " is running; assuming that it is not");
	        return false;
	    }
	}
	
	
	private Status getStatus() {
	    final Properties props;
	    try {
	        props = loadProperties();
	    } catch (final IOException ioe) {
	        return new Status(null, null, false, false);
	    }
	    
	    if ( props == null ) {
	        return new Status(null, null, false, false);
	    }
	    
        final String portValue = props.getProperty("port");
        final String pid = props.getProperty("pid");
        final String secretKey = props.getProperty("secret.key");
        
        if ( portValue == null && pid == null ) {
            return new Status(null, null, false, false);
        }
        
        Integer port = null;
        boolean pingSuccess = false;
        if ( portValue != null ) {
            try {
                port = Integer.parseInt(portValue);
                pingSuccess = isPingSuccessful(port, secretKey);
            } catch (final NumberFormatException nfe) {
                return new Status(null, null, false, false);
            }
        }
        
        if ( pingSuccess ) {
            return new Status(port, pid, true, true);
        }
        
        final boolean alive = (pid == null) ? false : isProcessRunning(pid);
        return new Status(port, pid, pingSuccess, alive);
	}
	
	public void status() throws IOException {
	    final Status status = getStatus();
	    if ( status.isRespondingToPing() ) {
	        logger.info("Apache NiFi is currently running, listening to Bootstrap on port " + status.getPort() + 
	                ", PID=" + (status.getPid() == null ? "unknkown" : status.getPid()));
	        return;
	    }

	    if ( status.isProcessRunning() ) {
	        logger.info("Apache NiFi is running at PID " + status.getPid() + " but is not responding to ping requests");
	        return;
	    }
	    
	    if ( status.getPort() == null ) {
	        logger.info("Apache NiFi is not running");
	        return;
	    }
	    
	    if ( status.getPid() == null ) {
	        logger.info("Apache NiFi is not responding to Ping requests. The process may have died or may be hung");
	    } else {
	        logger.info("Apache NiFi is not running");
	    }
	}
	
	
	public void stop() throws IOException {
		final Integer port = getCurrentPort();
		if ( port == null ) {
			System.out.println("Apache NiFi is not currently running");
			return;
		}
		
		final Properties nifiProps = loadProperties();
		final String secretKey = nifiProps.getProperty("secret.key");
		
		try (final Socket socket = new Socket()) {
		    logger.fine("Connecting to NiFi instance");
			socket.setSoTimeout(60000);
			socket.connect(new InetSocketAddress("localhost", port));
			logger.fine("Established connection to NiFi instance.");
			socket.setSoTimeout(60000);
			
			logger.fine("Sending SHUTDOWN Command to port " + port);
			final OutputStream out = socket.getOutputStream();
			out.write((SHUTDOWN_CMD + " " + secretKey + "\n").getBytes(StandardCharsets.UTF_8));
			out.flush();
			
			final InputStream in = socket.getInputStream();
			final BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			final String response = reader.readLine();
			
			logger.fine("Received response to SHUTDOWN command: " + response);
			
			if ( SHUTDOWN_CMD.equals(response) ) {
				logger.info("Apache NiFi has accepted the Shutdown Command and is shutting down now");
				
				final String pid = nifiProps.getProperty("pid");
				if ( pid != null ) {
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
			        
			        final long startWait = System.nanoTime();
			        while ( isProcessRunning(pid) ) {
			            logger.info("Waiting for Apache NiFi to finish shutting down...");
			            final long waitNanos = System.nanoTime() - startWait;
			            final long waitSeconds = TimeUnit.NANOSECONDS.toSeconds(waitNanos);
			            if ( waitSeconds >= gracefulShutdownSeconds && gracefulShutdownSeconds > 0 ) {
			                if ( isProcessRunning(pid) ) {
			                    logger.warning("NiFi has not finished shutting down after " + gracefulShutdownSeconds + " seconds. Killing process.");
			                    try {
			                        killProcessTree(pid);
			                    } catch (final IOException ioe) {
			                        logger.severe("Failed to kill Process with PID " + pid);
			                    }
			                }
			                break;
			            } else {
			                try {
			                    Thread.sleep(2000L);
			                } catch (final InterruptedException ie) {}
			            }
			        }
			        
			        logger.info("NiFi has finished shutting down.");
				}
				
				final File statusFile = getStatusFile();
				if ( !statusFile.delete() ) {
					logger.severe("Failed to delete status file " + statusFile + "; this file should be cleaned up manually");
				}
			} else {
				logger.severe("When sending SHUTDOWN command to NiFi, got unexpected response " + response);
			}
		} catch (final IOException ioe) {
		    logger.severe("Failed to send shutdown command to port " + port + " due to " + ioe);
			return;
		}
	}
	
	
	private static List<String> getChildProcesses(final String ppid) throws IOException {
	    final Process proc = Runtime.getRuntime().exec(new String[] {"ps", "-o", "pid", "--no-headers", "--ppid", ppid});
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
	
	private void killProcessTree(final String pid) throws IOException {
	    logger.fine("Killing Process Tree for PID " + pid);
	    
	    final List<String> children = getChildProcesses(pid);
	    logger.fine("Children of PID " + pid + ": " + children);
	    
	    for ( final String childPid : children ) {
	        killProcessTree(childPid);
	    }
	    
	    Runtime.getRuntime().exec(new String[] {"kill", "-9", pid});
	}
	
	public static boolean isAlive(final Process process) {
		try {
			process.exitValue();
			return false;
		} catch (final IllegalStateException | IllegalThreadStateException itse) {
			return true;
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void start(final boolean monitor) throws IOException, InterruptedException {
		final Integer port = getCurrentPort();
		if ( port != null ) {
			System.out.println("Apache NiFi is already running, listening to Bootstrap on port " + port);
			return;
		}
		
		final ProcessBuilder builder = new ProcessBuilder();

		if ( !bootstrapConfigFile.exists() ) {
			throw new FileNotFoundException(bootstrapConfigFile.getAbsolutePath());
		}
		
		final Properties properties = new Properties();
		try (final FileInputStream fis = new FileInputStream(bootstrapConfigFile)) {
			properties.load(fis);
		}
		
		final Map<String, String> props = new HashMap<>();
		props.putAll( (Map) properties );

		final String specifiedWorkingDir = props.get("working.dir");
		if ( specifiedWorkingDir != null ) {
			builder.directory(new File(specifiedWorkingDir));
		}

		final File bootstrapConfigAbsoluteFile = bootstrapConfigFile.getAbsoluteFile();
		final File binDir = bootstrapConfigAbsoluteFile.getParentFile();
		final File workingDir = binDir.getParentFile();
		
		if ( specifiedWorkingDir == null ) {
			builder.directory(workingDir);
		}
		
		final String libFilename = replaceNull(props.get("lib.dir"), "./lib").trim();
		File libDir = getFile(libFilename, workingDir);
		
		final String confFilename = replaceNull(props.get("conf.dir"), "./conf").trim();
		File confDir = getFile(confFilename, workingDir);
		
		String nifiPropsFilename = props.get("props.file");
		if ( nifiPropsFilename == null ) {
			if ( confDir.exists() ) {
				nifiPropsFilename = new File(confDir, "nifi.properties").getAbsolutePath();
			} else {
				nifiPropsFilename = DEFAULT_CONFIG_FILE;
			}
		}
		
		nifiPropsFilename = nifiPropsFilename.trim();
		
		final List<String> javaAdditionalArgs = new ArrayList<>();
		for ( final Map.Entry<String, String> entry : props.entrySet() ) {
			final String key = entry.getKey();
			final String value = entry.getValue();
			
			if ( key.startsWith("java.arg") ) {
				javaAdditionalArgs.add(value);
			}
		}
		
		final File[] libFiles = libDir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(final File dir, final String filename) {
				return filename.toLowerCase().endsWith(".jar");
			}
		});
		
		if ( libFiles == null || libFiles.length == 0 ) {
			throw new RuntimeException("Could not find lib directory at " + libDir.getAbsolutePath());
		}
		
		final File[] confFiles = confDir.listFiles();
		if ( confFiles == null || confFiles.length == 0 ) {
			throw new RuntimeException("Could not find conf directory at " + confDir.getAbsolutePath());
		}

		final List<String> cpFiles = new ArrayList<>(confFiles.length + libFiles.length);
		cpFiles.add(confDir.getAbsolutePath());
		for ( final File file : libFiles ) {
			cpFiles.add(file.getAbsolutePath());
		}
		
		final StringBuilder classPathBuilder = new StringBuilder();
		for (int i=0; i < cpFiles.size(); i++) {
			final String filename = cpFiles.get(i);
			classPathBuilder.append(filename);
			if ( i < cpFiles.size() - 1 ) {
				classPathBuilder.append(File.pathSeparatorChar);
			}
		}

		final String classPath = classPathBuilder.toString();
		String javaCmd = props.get("java");
		if ( javaCmd == null ) {
			javaCmd = "java";
		}
		
		final NiFiListener listener = new NiFiListener();
		final int listenPort = listener.start(this);
		
		String runAs = isWindows() ? null : props.get(RUN_AS_PROP);
		if ( runAs != null ) {
		    runAs = runAs.trim();
		    if ( runAs.isEmpty() ) {
		        runAs = null;
		    }
		}
		
		final List<String> cmd = new ArrayList<>();
		if ( runAs != null ) {
		    cmd.add("sudo");
		    cmd.add("-u");
		    cmd.add(runAs);
		}
		cmd.add(javaCmd);
		cmd.add("-classpath");
		cmd.add(classPath);
		cmd.addAll(javaAdditionalArgs);
		cmd.add("-Dnifi.properties.file.path=" + nifiPropsFilename);
		cmd.add("-Dnifi.bootstrap.listen.port=" + listenPort);
		cmd.add("-Dapp=NiFi");
		cmd.add("org.apache.nifi.NiFi");
		
		builder.command(cmd);
		
		final StringBuilder cmdBuilder = new StringBuilder();
		for ( final String s : cmd ) {
			cmdBuilder.append(s).append(" ");
		}

		logger.info("Starting Apache NiFi...");
		logger.info("Working Directory: " + workingDir.getAbsolutePath());
		logger.info("Command: " + cmdBuilder.toString());
		
		if ( monitor ) {
			String gracefulShutdown = props.get(GRACEFUL_SHUTDOWN_PROP);
			if ( gracefulShutdown == null ) {
				gracefulShutdown = DEFAULT_GRACEFUL_SHUTDOWN_VALUE;
			}

			final int gracefulShutdownSeconds;
			try {
				gracefulShutdownSeconds = Integer.parseInt(gracefulShutdown);
			} catch (final NumberFormatException nfe) {
				throw new NumberFormatException("The '" + GRACEFUL_SHUTDOWN_PROP + "' property in Bootstrap Config File " + bootstrapConfigAbsoluteFile.getAbsolutePath() + " has an invalid value. Must be a non-negative integer");
			}
			
			if ( gracefulShutdownSeconds < 0 ) {
				throw new NumberFormatException("The '" + GRACEFUL_SHUTDOWN_PROP + "' property in Bootstrap Config File " + bootstrapConfigAbsoluteFile.getAbsolutePath() + " has an invalid value. Must be a non-negative integer");
			}
			
			Process process = builder.start();
			Long pid = getPid(process);
		    if ( pid != null ) {
                nifiPid = pid;
                final Properties nifiProps = new Properties();
                nifiProps.setProperty("pid", String.valueOf(nifiPid));
                saveProperties(nifiProps);
            }
			
			shutdownHook = new ShutdownHook(process, this, secretKey, gracefulShutdownSeconds);
			final Runtime runtime = Runtime.getRuntime();
			runtime.addShutdownHook(shutdownHook);
			
			while (true) {
				final boolean alive = isAlive(process);
				
				if ( alive ) {
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
					
					if (autoRestartNiFi) {
						logger.warning("Apache NiFi appears to have died. Restarting...");
						process = builder.start();
						
						pid = getPid(process);
						if ( pid != null ) {
			                nifiPid = pid;
			                final Properties nifiProps = new Properties();
			                nifiProps.setProperty("pid", String.valueOf(nifiPid));
			                saveProperties(nifiProps);
			            }
						
						shutdownHook = new ShutdownHook(process, this, secretKey, gracefulShutdownSeconds);
						runtime.addShutdownHook(shutdownHook);
						
						final boolean started = waitForStart();
						
						if ( started ) {
							logger.info("Successfully started Apache NiFi" + (pid == null ? "" : " with PID " + pid));
						} else {
							logger.severe("Apache NiFi does not appear to have started");
						}
					} else {
						return;
					}
				}
			}
		} else {
			final Process process = builder.start();
			final Long pid = getPid(process);
			
			if ( pid != null ) {
			    nifiPid = pid;
                final Properties nifiProps = new Properties();
                nifiProps.setProperty("pid", String.valueOf(nifiPid));
                saveProperties(nifiProps);
			}
			
			boolean started = waitForStart();
			
			if ( started ) {
				logger.info("Successfully started Apache NiFi" + (pid == null ? "" : " with PID " + pid));
			} else {
				logger.severe("Apache NiFi does not appear to have started");
			}
			
			listener.stop();
		}
	}
	
	
	private Long getPid(final Process process) {
	    try {
            final Class<?> procClass = process.getClass();
            final Field pidField = procClass.getDeclaredField("pid");
            pidField.setAccessible(true);
            final Object pidObject = pidField.get(process);
            
            logger.fine("PID Object = " + pidObject);
            
            if ( pidObject instanceof Number ) {
                return ((Number) pidObject).longValue();
            }
            return null;
        } catch (final IllegalAccessException | NoSuchFieldException nsfe) {
            logger.fine("Could not find PID for child process due to " + nsfe);
            return null;
        }
	}
	
	private boolean isWindows() {
	    final String osName = System.getProperty("os.name");
	    return osName != null && osName.toLowerCase().contains("win");
	}
	
	private boolean waitForStart() {
		lock.lock();
		try {
			final long startTime = System.nanoTime();
			
			while ( ccPort < 1 ) {
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
		if ( !file.isAbsolute() ) {
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
	
	void setNiFiCommandControlPort(final int port, final String secretKey) {
		this.ccPort = port;
		this.secretKey = secretKey;
		
		if ( shutdownHook != null ) {
		    shutdownHook.setSecretKey(secretKey);
		}
		
		final File statusFile = getStatusFile();
		
		final Properties nifiProps = new Properties();
		if ( nifiPid != -1 ) {
		    nifiProps.setProperty("pid", String.valueOf(nifiPid));
		}
		nifiProps.setProperty("port", String.valueOf(ccPort));
		nifiProps.setProperty("secret.key", secretKey);
		
		try {
		    saveProperties(nifiProps);
		} catch (final IOException ioe) {
		    logger.warning("Apache NiFi has started but failed to persist NiFi Port information to " + statusFile.getAbsolutePath() + " due to " + ioe);
		}
		
		logger.info("Apache NiFi now running and listening for Bootstrap requests on port " + port);
	}
	
	int getNiFiCommandControlPort() {
		return this.ccPort;
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
