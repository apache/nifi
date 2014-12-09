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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class ShutdownHook extends Thread {
	private final Process nifiProcess;
	private final RunNiFi runner;
	
	public static final int WAIT_SECONDS = 10;
	
	public ShutdownHook(final Process nifiProcess, final RunNiFi runner) {
		this.nifiProcess = nifiProcess;
		this.runner = runner;
	}
	
	@Override
	public void run() {
		runner.setAutoRestartNiFi(false);
		final int ccPort = runner.getNiFiCommandControlPort();
		if ( ccPort > 0 ) {
			System.out.println("Initiating Shutdown of NiFi...");
			
			try {
				final Socket socket = new Socket("localhost", ccPort);
				final OutputStream out = socket.getOutputStream();
				out.write("SHUTDOWN\n".getBytes(StandardCharsets.UTF_8));
				out.flush();
				
				socket.close();
			} catch (final IOException ioe) {
				System.out.println("Failed to Shutdown NiFi due to " + ioe);
			}
		}
		
		try {
			nifiProcess.waitFor(WAIT_SECONDS, TimeUnit.SECONDS);
		} catch (final InterruptedException ie) {
		}

		if ( nifiProcess.isAlive() ) {
			System.out.println("NiFi has not finished shutting down after " + WAIT_SECONDS + " seconds. Killing process.");
		}
		nifiProcess.destroy();
		
		final File statusFile = runner.getStatusFile();
		if ( !statusFile.delete() ) {
			System.err.println("Failed to delete status file " + statusFile.getAbsolutePath());
		}
	}
}
