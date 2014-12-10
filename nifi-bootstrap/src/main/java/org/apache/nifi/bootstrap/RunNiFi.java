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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


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
	
	public static final int MAX_RESTART_ATTEMPTS = 5;
	public static final int STARTUP_WAIT_SECONDS = 60;
	
	public static final String SHUTDOWN_CMD = "SHUTDOWN";
	public static final String PING_CMD = "PING";
	
	private volatile boolean autoRestartNiFi = true;
	private volatile int ccPort = -1;
	
	private final Lock lock = new ReentrantLock();
	private final Condition startupCondition = lock.newCondition();
	
	private final File bootstrapConfigFile;
	
	public RunNiFi(final File bootstrapConfigFile) {
		this.bootstrapConfigFile = bootstrapConfigFile;
	}
	
	private static void printUsage() {
		System.out.println("Usage:");
		System.out.println();
		System.out.println("java org.apache.nifi.bootstrap.RunNiFi <command>");
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
		if ( args.length != 1 ) {
			printUsage();
			return;
		}
		
		switch (args[0].toLowerCase()) {
			case "start":
			case "run":
			case "stop":
			case "status":
				break;
			default:
				System.out.println("Invalid argument: " + args[0]);
				System.out.println();
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
		
		final RunNiFi runNiFi = new RunNiFi(configFile);
		
		switch (args[0].toLowerCase()) {
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
		final File statusFile = new File(bin, "nifi.port");
		return statusFile;
	}

	private Integer getCurrentPort() throws IOException {
		try {
			final File statusFile = getStatusFile();
			final byte[] info = Files.readAllBytes(statusFile.toPath());
			final String text = new String(info);
			
			final int port = Integer.parseInt(text);
			
			try (final Socket socket = new Socket("localhost", port)) {
				final OutputStream out = socket.getOutputStream();
				out.write((PING_CMD + "\n").getBytes(StandardCharsets.UTF_8));
				out.flush();
				
				final InputStream in = socket.getInputStream();
				final BufferedReader reader = new BufferedReader(new InputStreamReader(in));
				final String response = reader.readLine();
				if ( response.equals(PING_CMD) ) {
					return port;
				}
			} catch (final IOException ioe) {
				System.out.println("Found NiFi instance info at " + statusFile + " indicating that NiFi is running and listening to port " + port + " but unable to communicate with NiFi on that port. The process may have died or may be hung.");
				throw ioe;
			}
		} catch (final Exception e) {
			return null;
		}
		
		return null;
	}
	
	
	public void status() throws IOException {
		final Integer port = getCurrentPort();
		if ( port == null ) {
			System.out.println("Apache NiFi does not appear to be running");
		} else {
			System.out.println("Apache NiFi is currently running, listening on port " + port);
		}
		return;
	}
	
	
	public void stop() throws IOException {
		final Integer port = getCurrentPort();
		if ( port == null ) {
			System.out.println("Apache NiFi is not currently running");
			return;
		}
		
		try (final Socket socket = new Socket()) {
			socket.setSoTimeout(60000);
			socket.connect(new InetSocketAddress("localhost", port));
			socket.setSoTimeout(60000);
			
			final OutputStream out = socket.getOutputStream();
			out.write((SHUTDOWN_CMD + "\n").getBytes(StandardCharsets.UTF_8));
			out.flush();
			
			final InputStream in = socket.getInputStream();
			final BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			final String response = reader.readLine();
			if ( SHUTDOWN_CMD.equals(response) ) {
				System.out.println("Apache NiFi has accepted the Shutdown Command and is shutting down now");
				
				final File statusFile = getStatusFile();
				if ( !statusFile.delete() ) {
					System.err.println("Failed to delete status file " + statusFile + "; this file should be cleaned up manually");
				}
			} else {
				System.err.println("When sending SHUTDOWN command to NiFi, got unexpected response " + response);
			}
		} catch (final IOException ioe) {
			System.err.println("Failed to communicate with Apache NiFi");
			return;
		}
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
			System.out.println("Apache NiFi is already running, listening on port " + port);
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
		
		final List<String> cmd = new ArrayList<>();
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

		System.out.println("Starting Apache NiFi...");
		System.out.println("Working Directory: " + workingDir.getAbsolutePath());
		System.out.println("Command: " + cmdBuilder.toString());
		
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
			
			ShutdownHook shutdownHook = new ShutdownHook(process, this, gracefulShutdownSeconds);
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
						System.out.println("Apache NiFi appears to have died. Restarting...");
						process = builder.start();
						
						shutdownHook = new ShutdownHook(process, this, gracefulShutdownSeconds);
						runtime.addShutdownHook(shutdownHook);
						
						final boolean started = waitForStart();
						
						if ( started ) {
							System.out.println("Successfully started Apache NiFi");
						} else {
							System.err.println("Apache NiFi does not appear to have started");
						}
					} else {
						return;
					}
				}
			}
		} else {
			builder.start();
			boolean started = waitForStart();
			
			if ( started ) {
				System.out.println("Successfully started Apache NiFi");
			} else {
				System.err.println("Apache NiFi does not appear to have started");
			}
			
			listener.stop();
		}
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
	
	void setNiFiCommandControlPort(final int port) {
		this.ccPort = port;

		final File statusFile = getStatusFile();
		try (final FileOutputStream fos = new FileOutputStream(statusFile)) {
			fos.write(String.valueOf(port).getBytes(StandardCharsets.UTF_8));
			fos.getFD().sync();
		} catch (final IOException ioe) {
			System.err.println("Apache NiFi has started but failed to persist NiFi Port information to " + statusFile.getAbsolutePath() + " due to " + ioe);
		}
		
		System.out.println("Apache NiFi now running and listening for requests on port " + port);
	}
	
	int getNiFiCommandControlPort() {
		return this.ccPort;
	}
}
