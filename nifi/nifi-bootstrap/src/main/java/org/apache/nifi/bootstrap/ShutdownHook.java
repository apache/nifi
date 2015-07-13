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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ShutdownHook extends Thread {

    private final Process nifiProcess;
    private final RunNiFi runner;
    private final int gracefulShutdownSeconds;
    private final ExecutorService executor;

    private volatile String secretKey;

    public ShutdownHook(final Process nifiProcess, final RunNiFi runner, final String secretKey, final int gracefulShutdownSeconds, final ExecutorService executor) {
        this.nifiProcess = nifiProcess;
        this.runner = runner;
        this.secretKey = secretKey;
        this.gracefulShutdownSeconds = gracefulShutdownSeconds;
        this.executor = executor;
    }

    void setSecretKey(final String secretKey) {
        this.secretKey = secretKey;
    }

    @Override
    public void run() {
        executor.shutdown();
        runner.setAutoRestartNiFi(false);
        final int ccPort = runner.getNiFiCommandControlPort();
        if (ccPort > 0) {
            System.out.println("Initiating Shutdown of NiFi...");

            try {
                final Socket socket = new Socket("localhost", ccPort);
                final OutputStream out = socket.getOutputStream();
                out.write(("SHUTDOWN " + secretKey + "\n").getBytes(StandardCharsets.UTF_8));
                out.flush();

                socket.close();
            } catch (final IOException ioe) {
                System.out.println("Failed to Shutdown NiFi due to " + ioe);
            }
        }

        System.out.println("Waiting for Apache NiFi to finish shutting down...");
        final long startWait = System.nanoTime();
        while (RunNiFi.isAlive(nifiProcess)) {
            final long waitNanos = System.nanoTime() - startWait;
            final long waitSeconds = TimeUnit.NANOSECONDS.toSeconds(waitNanos);
            if (waitSeconds >= gracefulShutdownSeconds && gracefulShutdownSeconds > 0) {
                if (RunNiFi.isAlive(nifiProcess)) {
                    System.out.println("NiFi has not finished shutting down after " + gracefulShutdownSeconds + " seconds. Killing process.");
                    nifiProcess.destroy();
                }
                break;
            } else {
                try {
                    Thread.sleep(1000L);
                } catch (final InterruptedException ie) {
                }
            }
        }

        final File statusFile = runner.getStatusFile();
        if (!statusFile.delete()) {
            System.err.println("Failed to delete status file " + statusFile.getAbsolutePath() + "; this file should be cleaned up manually");
        }
    }
}
