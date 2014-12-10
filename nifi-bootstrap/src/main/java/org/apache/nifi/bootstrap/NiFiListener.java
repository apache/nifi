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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class NiFiListener {
	private ServerSocket serverSocket;
	private volatile Listener listener;
	
	int start(final RunNiFi runner) throws IOException {
		serverSocket = new ServerSocket();
		serverSocket.bind(new InetSocketAddress("localhost", 0));
		
		final int localPort = serverSocket.getLocalPort();
		listener = new Listener(serverSocket, runner);
		final Thread listenThread = new Thread(listener);
		listenThread.setName("Listen to NiFi");
		listenThread.start();
		return localPort;
	}
	
	public void stop() throws IOException {
		final Listener listener = this.listener;
		if ( listener == null ) {
			return;
		}
		
		listener.stop();
	}
	
	private class Listener implements Runnable {
		private final ServerSocket serverSocket;
		private final ExecutorService executor;
		private final RunNiFi runner;
		private volatile boolean stopped = false;
		
		public Listener(final ServerSocket serverSocket, final RunNiFi runner) {
			this.serverSocket = serverSocket;
			this.executor = Executors.newFixedThreadPool(2);
			this.runner = runner;
		}
		
		public void stop() throws IOException {
			stopped = true;
			
			executor.shutdown();
			try {
				executor.awaitTermination(3, TimeUnit.SECONDS);
			} catch (final InterruptedException ie) {
			}
			
			serverSocket.close();
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
								final BootstrapCodec codec = new BootstrapCodec(runner, socket.getInputStream(), socket.getOutputStream());
								codec.communicate();
								socket.close();
							} catch (final Throwable t) {
								System.out.println("Failed to communicate with NiFi due to " + t);
								t.printStackTrace();
							}
						}
					});
				} catch (final Throwable t) {
					System.err.println("Failed to receive information from NiFi due to " + t);
					t.printStackTrace();
				}
			}
		}
	}
}
