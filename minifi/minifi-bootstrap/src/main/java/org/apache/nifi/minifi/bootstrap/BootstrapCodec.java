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

import org.apache.nifi.minifi.bootstrap.exception.InvalidCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;

public class BootstrapCodec {

    private final RunMiNiFi runner;
    private final BufferedReader reader;
    private final BufferedWriter writer;
    private final Logger logger = LoggerFactory.getLogger(BootstrapCodec.class);

    public BootstrapCodec(final RunMiNiFi runner, final InputStream in, final OutputStream out) {
        this.runner = runner;
        this.reader = new BufferedReader(new InputStreamReader(in));
        this.writer = new BufferedWriter(new OutputStreamWriter(out));
    }

    public void communicate() throws IOException {
        final String line = reader.readLine();
        final String[] splits = line.split(" ");
        if (splits.length < 0) {
            throw new IOException("Received invalid command from MiNiFi: " + line);
        }

        final String cmd = splits[0];
        final String[] args;
        if (splits.length == 1) {
            args = new String[0];
        } else {
            args = Arrays.copyOfRange(splits, 1, splits.length);
        }

        try {
            processRequest(cmd, args);
        } catch (final InvalidCommandException ice) {
            throw new IOException("Received invalid command from MiNiFi: " + line + " : " + (ice.getMessage() == null ? "" : "Details: " + ice.toString()));
        }
    }

    private void processRequest(final String cmd, final String[] args) throws InvalidCommandException, IOException {
        switch (cmd) {
            case "PORT": {
                logger.debug("Received 'PORT' command from MINIFI");
                if (args.length != 2) {
                    throw new InvalidCommandException();
                }

                final int port;
                try {
                    port = Integer.parseInt(args[0]);
                } catch (final NumberFormatException nfe) {
                    throw new InvalidCommandException("Invalid Port number; should be integer between 1 and 65535");
                }

                if (port < 1 || port > 65535) {
                    throw new InvalidCommandException("Invalid Port number; should be integer between 1 and 65535");
                }

                final String secretKey = args[1];

                runner.setMiNiFiCommandControlPort(port, secretKey);
                writer.write("OK");
                writer.newLine();
                writer.flush();
            }
            break;
            case "STARTED": {
                logger.debug("Received 'STARTED' command from MINIFI");
                if (args.length != 1) {
                    throw new InvalidCommandException("STARTED command must contain a status argument");
                }

                if (!"true".equals(args[0]) && !"false".equals(args[0])) {
                    throw new InvalidCommandException("Invalid status for STARTED command; should be true or false, but was '" + args[0] + "'");
                }

                final boolean started = Boolean.parseBoolean(args[0]);
                runner.setNiFiStarted(started);
                writer.write("OK");
                writer.newLine();
                writer.flush();
            }
            break;
            case "SHUTDOWN": {
                logger.debug("Received 'SHUTDOWN' command from MINIFI");
                runner.shutdownChangeNotifier();
                runner.shutdownPeriodicStatusReporters();
                writer.write("OK");
                writer.newLine();
                writer.flush();
            }
            break;
        }
    }
}
