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
package org.apache.nifi.websocket.example;

import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * This is a WebSocket client example testcase.
 */
@Ignore
public class WebSocketClientExample {

    private static Logger logger = LoggerFactory.getLogger(WebSocketClientExample.class);

    @Test
    public void test() {
        String destUri = "wss://localhost:50010/test";

        final CountDownLatch replyLatch = new CountDownLatch(1);
        final SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setKeyStorePath("src/test/resources/certs/keystore.jks");
        sslContextFactory.setKeyStorePassword("passwordpassword");
        sslContextFactory.setKeyStoreType("JKS");
        sslContextFactory.setTrustStorePath("src/test/resources/certs/truststore.jks");
        sslContextFactory.setTrustStorePassword("passwordpassword");
        sslContextFactory.setTrustStoreType("JKS");

        WebSocketClient client = new WebSocketClient(sslContextFactory);
        WebSocketAdapter socket = new WebSocketAdapter() {
            @Override
            public void onWebSocketConnect(Session session) {
                super.onWebSocketConnect(session);

                try {
                    session.getRemote().sendString("Hello, this is Jetty ws client.");
                } catch (IOException e) {
                    logger.error("Failed to send a message due to " + e, e);
                }
            }

            @Override
            public void onWebSocketText(String message) {
                logger.info("Received a reply: {}", message);
                replyLatch.countDown();
            }
        };
        try {
            client.start();

            URI echoUri = new URI(destUri);
            ClientUpgradeRequest request = new ClientUpgradeRequest();
            final Future<Session> connect = client.connect(socket, echoUri, request);
            logger.info("Connecting to : {}", echoUri);

            final Session session = connect.get(3, TimeUnit.SECONDS);
            logger.info("Connected, session={}", session);

            session.close(StatusCode.NORMAL, "Bye");

        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            try {
                client.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
