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
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.nifi.util.LimitingInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BootstrapListener {

    private static final Logger logger = LoggerFactory.getLogger(BootstrapListener.class);

    private final NiFi nifi;
    private final int bootstrapPort;
    private final String secretKey;

    private volatile Listener listener;
    private volatile ServerSocket serverSocket;

    public BootstrapListener(final NiFi nifi, final int bootstrapPort) {
        this.nifi = nifi;
        this.bootstrapPort = bootstrapPort;
        secretKey = UUID.randomUUID().toString();
    }

    public void start() throws IOException {
        logger.debug("Starting Bootstrap Listener to communicate with Bootstrap Port {}", bootstrapPort);

        serverSocket = new ServerSocket();
        serverSocket.bind(new InetSocketAddress("localhost", 0));
        serverSocket.setSoTimeout(2000);

        final int localPort = serverSocket.getLocalPort();
        logger.info("Started Bootstrap Listener, Listening for incoming requests on port {}", localPort);

        listener = new Listener(serverSocket);
        final Thread listenThread = new Thread(listener);
        listenThread.setDaemon(true);
        listenThread.setName("Listen to Bootstrap");
        listenThread.start();

        logger.debug("Notifying Bootstrap that local port is {}", localPort);
        sendCommand("PORT", new String[] { String.valueOf(localPort), secretKey});
    }

    public void stop() {
        if (listener != null) {
            listener.stop();
        }
    }

    public void sendStartedStatus(boolean status) throws IOException {
        logger.debug("Notifying Bootstrap that the status of starting NiFi is {}", status);
        sendCommand("STARTED", new String[]{ String.valueOf(status) });
    }

    private void sendCommand(final String command, final String[] args) throws IOException {
        try (final Socket socket = new Socket()) {
            socket.setSoTimeout(60000);
            socket.connect(new InetSocketAddress("localhost", bootstrapPort));
            socket.setSoTimeout(60000);

            final StringBuilder commandBuilder = new StringBuilder(command);
            for (final String arg : args) {
                commandBuilder.append(" ").append(arg);
            }
            commandBuilder.append("\n");

            final String commandWithArgs = commandBuilder.toString();
            logger.debug("Sending command to Bootstrap: " + commandWithArgs);

            final OutputStream out = socket.getOutputStream();
            out.write((commandWithArgs).getBytes(StandardCharsets.UTF_8));
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
            while (!stopped) {
                try {
                    final Socket socket;
                    try {
                        logger.debug("Listening for Bootstrap Requests");
                        socket = serverSocket.accept();
                    } catch (final SocketTimeoutException ste) {
                        if (stopped) {
                            return;
                        }

                        continue;
                    } catch (final IOException ioe) {
                        if (stopped) {
                            return;
                        }

                        throw ioe;
                    }

                    logger.debug("Received connection from Bootstrap");
                    socket.setSoTimeout(5000);

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
                                    case DUMP:
                                        logger.info("Received DUMP request from Bootstrap");
                                        writeDump(socket.getOutputStream());
                                        break;
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

    private static void writeDump(final OutputStream out) throws IOException {
        final ThreadMXBean mbean = ManagementFactory.getThreadMXBean();
        final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));

        final ThreadInfo[] infos = mbean.dumpAllThreads(true, true);
        final long[] deadlockedThreadIds = mbean.findDeadlockedThreads();
        final long[] monitorDeadlockThreadIds = mbean.findMonitorDeadlockedThreads();

        final List<ThreadInfo> sortedInfos = new ArrayList<>(infos.length);
        for (final ThreadInfo info : infos) {
            sortedInfos.add(info);
        }
        Collections.sort(sortedInfos, new Comparator<ThreadInfo>() {
            @Override
            public int compare(ThreadInfo o1, ThreadInfo o2) {
                return o1.getThreadName().toLowerCase().compareTo(o2.getThreadName().toLowerCase());
            }
        });

        final StringBuilder sb = new StringBuilder();
        for (final ThreadInfo info : sortedInfos) {
            sb.append("\n");
            sb.append("\"").append(info.getThreadName()).append("\" Id=");
            sb.append(info.getThreadId()).append(" ");
            sb.append(info.getThreadState().toString()).append(" ");

            switch (info.getThreadState()) {
                case BLOCKED:
                case TIMED_WAITING:
                case WAITING:
                    sb.append(" on ");
                    sb.append(info.getLockInfo());
                    break;
                default:
                    break;
            }

            if (info.isSuspended()) {
                sb.append(" (suspended)");
            }
            if (info.isInNative()) {
                sb.append(" (in native code)");
            }

            if (deadlockedThreadIds != null && deadlockedThreadIds.length > 0) {
                for (final long id : deadlockedThreadIds) {
                    if (id == info.getThreadId()) {
                        sb.append(" ** DEADLOCKED THREAD **");
                    }
                }
            }

            if (monitorDeadlockThreadIds != null && monitorDeadlockThreadIds.length > 0) {
                for (final long id : monitorDeadlockThreadIds) {
                    if (id == info.getThreadId()) {
                        sb.append(" ** MONITOR-DEADLOCKED THREAD **");
                    }
                }
            }

            final StackTraceElement[] stackTraces = info.getStackTrace();
            for (final StackTraceElement element : stackTraces) {
                sb.append("\n\tat ").append(element);

                final MonitorInfo[] monitors = info.getLockedMonitors();
                for (final MonitorInfo monitor : monitors) {
                    if (monitor.getLockedStackFrame().equals(element)) {
                        sb.append("\n\t- waiting on ").append(monitor);
                    }
                }
            }

            final LockInfo[] lockInfos = info.getLockedSynchronizers();
            if (lockInfos.length > 0) {
                sb.append("\n\t");
                sb.append("Number of Locked Synchronizers: ").append(lockInfos.length);
                for (final LockInfo lockInfo : lockInfos) {
                    sb.append("\n\t- ").append(lockInfo.toString());
                }
            }

            sb.append("\n");
        }

        if (deadlockedThreadIds != null && deadlockedThreadIds.length > 0) {
            sb.append("\n\nDEADLOCK DETECTED!");
            sb.append("\nThe following thread IDs are deadlocked:");
            for (final long id : deadlockedThreadIds) {
                sb.append("\n").append(id);
            }
        }

        if (monitorDeadlockThreadIds != null && monitorDeadlockThreadIds.length > 0) {
            sb.append("\n\nMONITOR DEADLOCK DETECTED!");
            sb.append("\nThe following thread IDs are deadlocked:");
            for (final long id : monitorDeadlockThreadIds) {
                sb.append("\n").append(id);
            }
        }

        writer.write(sb.toString());
        writer.flush();
    }

    private void echoPing(final OutputStream out) throws IOException {
        out.write("PING\n".getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    private void echoShutdown(final OutputStream out) throws IOException {
        out.write("SHUTDOWN\n".getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    @SuppressWarnings("resource")  // we don't want to close the stream, as the caller will do that
    private BootstrapRequest readRequest(final InputStream in) throws IOException {
        // We want to ensure that we don't try to read data from an InputStream directly
        // by a BufferedReader because any user on the system could open a socket and send
        // a multi-gigabyte file without any new lines in order to crash the NiFi instance
        // (or at least cause OutOfMemoryErrors, which can wreak havoc on the running instance).
        // So we will limit the Input Stream to only 4 KB, which should be plenty for any request.
        final LimitingInputStream limitingIn = new LimitingInputStream(in, 4096);
        final BufferedReader reader = new BufferedReader(new InputStreamReader(limitingIn));

        final String line = reader.readLine();
        final String[] splits = line.split(" ");
        if (splits.length < 1) {
            throw new IOException("Received invalid request from Bootstrap: " + line);
        }

        final String requestType = splits[0];
        final String[] args;
        if (splits.length == 1) {
            throw new IOException("Received invalid request from Bootstrap; request did not have a secret key; request type = " + requestType);
        } else if (splits.length == 2) {
            args = new String[0];
        } else {
            args = Arrays.copyOfRange(splits, 2, splits.length);
        }

        final String requestKey = splits[1];
        if (!secretKey.equals(requestKey)) {
            throw new IOException("Received invalid Secret Key for request type " + requestType);
        }

        try {
            return new BootstrapRequest(requestType, args);
        } catch (final Exception e) {
            throw new IOException("Received invalid request from Bootstrap; request type = " + requestType);
        }
    }

    private static class BootstrapRequest {

        public static enum RequestType {

            SHUTDOWN,
            DUMP,
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

        @SuppressWarnings("unused")
        public String[] getArgs() {
            return args;
        }
    }
}
