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
package org.apache.nifi.cluster.manager.testutils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.cluster.manager.testutils.HttpRequest.HttpRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple HTTP web server that allows clients to register canned-responses to respond to received requests.
 *
 */
public class HttpServer {

    private static final Logger logger = LoggerFactory.getLogger(HttpServer.class);

    private final ExecutorService executorService;
    private final ServerSocket serverSocket;
    private final Queue<HttpResponseAction> responseQueue = new ConcurrentLinkedQueue<>();
    private final Map<String, String> checkedHeaders = new HashMap<>();
    private final Map<String, List<String>> checkedParameters = new HashMap<>();
    private final int port;

    public HttpServer(int numThreads, int port) throws IOException {
        this.port = port;
        executorService = Executors.newFixedThreadPool(numThreads);
        serverSocket = new ServerSocket(port);
    }

    public void start() {

        new Thread() {
            @Override
            public void run() {
                while (isRunning()) {
                    try {
                        final Socket conn = serverSocket.accept();
                        executorService.execute(new Runnable() {
                            @Override
                            public void run() {
                                handleRequest(conn);
                                if (conn.isClosed() == false) {
                                    try {
                                        conn.close();
                                    } catch (IOException ioe) {
                                    }
                                }
                            }
                        });
                    } catch (final SocketException se) {
                        /* ignored */
                    } catch (final IOException ioe) {
                        if (logger.isDebugEnabled()) {
                            logger.warn("", ioe);
                        }
                    }
                }
            }
        ;
    }

    .start();
    }

    public boolean isRunning() {
        return executorService.isShutdown() == false;
    }

    public void stop() {
        // shutdown server socket
        try {
            if (serverSocket.isClosed() == false) {
                serverSocket.close();
            }
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }

        // shutdown executor service
        try {
            executorService.shutdown();
            executorService.awaitTermination(3, TimeUnit.SECONDS);
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public int getPort() {
        if (isRunning()) {
            return serverSocket.getLocalPort();
        } else {
            return port;
        }
    }

    public Queue<HttpResponseAction> addResponseAction(final HttpResponseAction response) {
        responseQueue.add(response);
        return responseQueue;
    }

    public void addCheckedHeaders(final Map<String, String> headers) {
        checkedHeaders.putAll(headers);
    }

    public void addCheckedParameters(final Map<String, List<String>> parameters) {
        checkedParameters.putAll(parameters);
    }

    private void handleRequest(final Socket conn) {
        try {

            final HttpRequest httpRequest = buildRequest(conn.getInputStream());

            if (logger.isDebugEnabled()) {
                logger.debug("\n" + httpRequest);
            }

            // check headers
            final Map<String, String> reqHeaders = httpRequest.getHeaders();
            for (final Map.Entry<String, String> entry : checkedHeaders.entrySet()) {
                if (reqHeaders.containsKey(entry.getKey())) {
                    if (entry.getValue().equals(reqHeaders.get(entry.getKey()))) {
                        logger.error("Incorrect HTTP request header value received for checked header: " + entry.getKey());
                        conn.close();
                        return;
                    }
                } else {
                    logger.error("Missing checked header: " + entry.getKey());
                    conn.close();
                    return;
                }
            }

            // check parameters
            final Map<String, List<String>> reqParams = httpRequest.getParameters();
            for (final Map.Entry<String, List<String>> entry : checkedParameters.entrySet()) {
                if (reqParams.containsKey(entry.getKey())) {
                    if (entry.getValue().equals(reqParams.get(entry.getKey())) == false) {
                        logger.error("Incorrect HTTP request parameter values received for checked parameter: " + entry.getKey());
                        conn.close();
                        return;
                    }
                } else {
                    logger.error("Missing checked parameter: " + entry.getKey());
                    conn.close();
                    return;
                }
            }

            // apply the next response
            final HttpResponseAction response = responseQueue.remove();
            response.apply();

            // send the response to client
            final PrintWriter pw = new PrintWriter(conn.getOutputStream(), true);

            if (logger.isDebugEnabled()) {
                logger.debug("\n" + response.getResponse());
            }

            pw.print(response.getResponse());
            pw.flush();

        } catch (IOException ioe) { /* ignored */ }
    }

    private HttpRequest buildRequest(final InputStream requestIs) throws IOException {
        return new HttpRequestReader().read(new InputStreamReader(requestIs));
    }

    // reads an HTTP request from the given reader
    private class HttpRequestReader {

        public HttpRequest read(final Reader reader) throws IOException {

            HttpRequestBuilder builder = null;
            String line = "";
            boolean isRequestLine = true;
            while ((line = readLine(reader)).isEmpty() == false) {
                if (isRequestLine) {
                    builder = HttpRequest.createFromRequestLine(line);
                    isRequestLine = false;
                } else {
                    builder.addHeader(line);
                }
            }

            if (builder != null) {
                builder.addBody(reader);
            }

            return builder.build();
        }

        private String readLine(final Reader reader) throws IOException {

            /* read character at time to prevent blocking */
            final StringBuilder strb = new StringBuilder();
            char c;
            while ((c = (char) reader.read()) != '\n') {
                if (c != '\r') {
                    strb.append(c);
                }
            }
            return strb.toString();
        }
    }
}
