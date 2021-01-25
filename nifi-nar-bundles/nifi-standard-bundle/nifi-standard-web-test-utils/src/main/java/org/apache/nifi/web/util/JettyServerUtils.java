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
package org.apache.nifi.web.util;

import org.apache.nifi.security.util.ClientAuth;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import javax.net.ssl.SSLContext;

public class JettyServerUtils {

    private static final long IDLE_TIMEOUT = 60000;

    private static final long SERVER_START_SLEEP = 100L;

    public static Server createServer(final int port, final SSLContext sslContext, final ClientAuth clientAuth) {
        final Server server = new Server();

        final ServerConnector connector;
        if (sslContext == null) {
            connector = new ServerConnector(server);
        } else {
            final SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
            sslContextFactory.setSslContext(sslContext);

            if (ClientAuth.REQUIRED.equals(clientAuth)) {
                sslContextFactory.setNeedClientAuth(true);
            }

            connector = new ServerConnector(server, sslContextFactory);
        }

        connector.setPort(port);
        connector.setIdleTimeout(IDLE_TIMEOUT);
        server.addConnector(connector);

        final HandlerCollection handlerCollection = new HandlerCollection(true);
        server.setHandler(handlerCollection);
        return server;
    }

    public static void startServer(final Server server) throws Exception {
        server.start();
        while (!server.isStarted()) {
            Thread.sleep(SERVER_START_SLEEP);
        }
    }

    public static void addHandler(final Server server, final Handler handler) {
        final Handler serverHandler = server.getHandler();
        if (serverHandler instanceof HandlerCollection) {
            final HandlerCollection handlerCollection = (HandlerCollection) serverHandler;
            handlerCollection.addHandler(handler);
        }
    }

    public static void clearHandlers(final Server server) {
        final Handler serverHandler = server.getHandler();
        if (serverHandler instanceof HandlerCollection) {
            final HandlerCollection handlerCollection = (HandlerCollection) serverHandler;
            final Handler[] handlers = handlerCollection.getHandlers();
            if (handlers != null) {
                for (final Handler handler : handlerCollection.getHandlers()) {
                    handlerCollection.removeHandler(handler);
                }
            }

        }
    }
}
