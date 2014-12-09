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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BootstrapListener {
	private static final Logger logger = LoggerFactory.getLogger(BootstrapListener.class);
	
	private final NiFi nifi;
	private final int bootstrapPort;

	private Listener listener;
	private ServerSocket serverSocket;
	
	
	public BootstrapListener(final NiFi nifi, final int port) {
		this.nifi = nifi;
		this.bootstrapPort = port;
	}
	
	public void start() throws IOException {
		logger.debug("Starting Bootstrap Listener to communicate with Bootstrap Port {}", bootstrapPort);
		
		serverSocket = new ServerSocket();
		serverSocket.bind(new InetSocketAddress("localhost", 0));
		
		final int localPort = serverSocket.getLocalPort();
		logger.info("Started Bootstrap Listener, Listening for incoming requests on port {}", localPort);
		
		listener = new Listener(serverSocket);
		final Thread listenThread = new Thread(listener);
		listenThread.setName("Listen to Bootstrap");
		listenThread.start();
		
		logger.debug("Notifying Bootstrap that local port is {}", localPort);
		try (final Socket socket = new Socket()) {
			socket.setSoTimeout(60000);
			socket.connect(new InetSocketAddress("localhost", bootstrapPort));
			socket.setSoTimeout(60000);
			
			final OutputStream out = socket.getOutputStream();
			out.write(("PORT " + localPort + "\n").getBytes(StandardCharsets.UTF_8));
			out.flush();
			
			logger.debug("Awaiting response from Bootstrap...");
			final BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			final String response = reader.readLine();
			if ("OK".equals(response)) {
				logger.info("Successfully initiated communication with Bootstrap");
			} else {
				logger.error("Failed to communicate with Bootstrap. Bootstrap may be unable to issue or receive commands from NiFi");
			}
		}
	}
	
	
	public void stop() {
		if (listener != null) {
			listener.stop();
		}
	}
	
	private class Listener implements Runnable {
		private final ServerSocket serverSocket;
		private final ExecutorService executor;
		private volatile boolean stopped = false;
		
		public Listener(final ServerSocket serverSocket) {
			this.serverSocket = serverSocket;
			this.executor = Executors.newFixedThreadPool(2);
		}
		
		public void stop() {
			stopped = true;
			
			executor.shutdownNow();
			
			try {
				serverSocket.close();
			} catch (final IOException ioe) {
				// nothing to really do here. we could log this, but it would just become
				// confusing in the logs, as we're shutting down and there's no real benefit
			}
		}
		
		@Override
		public void run() {
			while (!serverSocket.isClosed()) {
				try {
					if ( stopped ) {
						return;
					}
					
					final Socket socket;
					try {
						socket = serverSocket.accept();
					} catch (final IOException ioe) {
						if ( stopped ) {
							return;
						}
						
						throw ioe;
					}
					
					executor.submit(new Runnable() {
						@Override
						public void run() {
							try {
								final BootstrapRequest request = readRequest(socket.getInputStream());
								final BootstrapRequest.RequestType requestType = request.getRequestType();
								
								switch (requestType) {
									case PING:
										logger.debug("Received PING request from Bootstrap; responding");
										echoPing(socket.getOutputStream());
										logger.debug("Responded to PING request from Bootstrap");
										break;
									case SHUTDOWN:
										logger.info("Received SHUTDOWN request from Bootstrap");
										echoShutdown(socket.getOutputStream());
										nifi.shutdownHook();
										return;
								}
							} catch (final Throwable t) {
								logger.error("Failed to process request from Bootstrap due to " + t.toString(), t);
							} finally {
								try {
									socket.close();
								} catch (final IOException ioe) {
									logger.warn("Failed to close socket to Bootstrap due to {}", ioe.toString());
								}
							}
						}
					});
				} catch (final Throwable t) {
					logger.error("Failed to process request from Bootstrap due to " + t.toString(), t);
				}
			}
		}
	}
	
	
	private void echoPing(final OutputStream out) throws IOException {
		out.write("PING\n".getBytes(StandardCharsets.UTF_8));
		out.flush();
	}
	
	private void echoShutdown(final OutputStream out) throws IOException {
		out.write("SHUTDOWN\n".getBytes(StandardCharsets.UTF_8));
		out.flush();
	}
	
	private BootstrapRequest readRequest(final InputStream in) throws IOException {
		final BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		
		final String line = reader.readLine();
		final String[] splits = line.split(" ");
		if ( splits.length < 0 ) {
			throw new IOException("Received invalid command from NiFi: " + line);
		}
		
		final String requestType = splits[0];
		final String[] args;
		if ( splits.length == 1 ) {
			args = new String[0];
		} else {
			args = Arrays.copyOfRange(splits, 1, splits.length);
		}
		
		try {
			return new BootstrapRequest(requestType, args);
		} catch (final Exception e) {
			throw new IOException("Received invalid request from bootstrap; request type = " + requestType);
		}
	}
	
	
	private static class BootstrapRequest {
		public static enum RequestType {
			SHUTDOWN,
			PING;
		}
		
		private final RequestType requestType;
		private final String[] args;
		
		public BootstrapRequest(final String request, final String[] args) {
			this.requestType = RequestType.valueOf(request);
			this.args = args;
		}
		
		public RequestType getRequestType() {
			return requestType;
		}
		
		public String[] getArgs() {
			return args;
		}
	}
}
