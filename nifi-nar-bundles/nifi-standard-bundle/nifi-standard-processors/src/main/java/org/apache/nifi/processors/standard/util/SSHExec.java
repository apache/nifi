/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.nifi.processors.standard.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * Executes a command on a remote machine via ssh.
 */
public class SSHExec {

    private static final int BUFFER_SIZE = 8192;
    private static final int RETRY_INTERVAL = 500;

    /** the command to execute via ssh */
    private String command = null;

    /** units are milliseconds, default is 0=infinite */
    private long maxwait = 0;

    /** for waiting for the command to finish */
    private Thread thread = null;

    private boolean usePty = false;

    private static final String TIMEOUT_MESSAGE =
            "Timeout period exceeded, connection dropped.";

    /**
     * Sets the command to execute on the remote host.
     *
     * @param command  The new command value
     */
    public void setCommand(final String command) {
        this.command = command;
    }

    /**
     * The connection can be dropped after a specified number of
     * milliseconds. This is sometimes useful when a connection may be
     * flaky. Default is 0, which means &quot;wait forever&quot;.
     *
     * @param timeout  The new timeout value in seconds
     */
    public void setTimeout(final long timeout) {
        maxwait = timeout;
    }

    /**
     * Whether a pseudo-tty should be allocated.
     * @param b boolean
     */
    public void setUsePty(final boolean b) {
        usePty = b;
    }

    /**
     * Execute the command on the remote host.
     *
     * @exception IOException  Most likely a network error or bad parameter.
     */
    public String execute(Session session) throws ProcessException,IOException {
        if (command == null ) {
            throw new ProcessException("Command or commandResource is required.");
        }

        final StringBuilder output = new StringBuilder();
        try {
            executeCommand(session, command, output);

            return output.toString();
        } catch (final ProcessException e) {
            throw e;
        }
    }

    private void executeCommand(final Session session, final String cmd, final StringBuilder sb)
            throws ProcessException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ByteArrayOutputStream errout = new ByteArrayOutputStream();

        try {
            final ChannelExec channel;
            /* execute the command */
            channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(cmd);
            channel.setOutputStream(out);
            channel.setExtOutputStream(out);
            channel.setErrStream(errout);

            channel.setPty(usePty);
            channel.connect();
            // wait for it to finish
            thread =
                    new Thread(() -> {
                        while (!channel.isClosed()) {
                            if (thread == null) {
                                return;
                            }
                            try {
                                Thread.sleep(RETRY_INTERVAL);
                            } catch (final Exception e) {
                                // ignored
                            }
                        }
                    });

            thread.start();
            thread.join(maxwait);

            if (thread.isAlive()) {
                // ran out of time
                thread = null;
                throw new ProcessException(TIMEOUT_MESSAGE);
            }
        } catch (final ProcessException e) {
            throw e;
        } catch (final JSchException e) {
            if (e.getMessage().contains("session is down")) {
                throw new ProcessException(TIMEOUT_MESSAGE, e);
            } else {
                throw new ProcessException(e);
            }
        } catch (final Exception e) {
            throw new ProcessException(e);
        } finally {
            //Save output for return to requestor
            sb.append(out.toString());
        }
    }

    /**
     * Writes a string to a file. If destination file exists, it may be
     * overwritten depending on the "append" value.
     *
     * @param from           string to write
     * @param to             file to write to
     * @param append         if true, append to existing file, else overwrite
     */
    private void writeToFile(final String from, final boolean append, final File to)
            throws IOException {
        try (FileWriter out = new FileWriter(to.getAbsolutePath(), append)) {
            final StringReader in = new StringReader(from);
            final char[] buffer = new char[BUFFER_SIZE];
            while (true) {
                int bytesRead = in.read(buffer);
                if (bytesRead == -1) {
                    break;
                }
                out.write(buffer, 0, bytesRead);
            }
            out.flush();
        }
    }

}
