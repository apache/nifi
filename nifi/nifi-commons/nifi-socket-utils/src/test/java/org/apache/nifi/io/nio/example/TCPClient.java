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
package org.apache.nifi.io.nio.example;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author none
 */
public class TCPClient {

    private static final Logger logger = LoggerFactory.getLogger(TCPClient.class);

    public static void main(final String[] args) throws Exception {
        final byte[] bytes = TCPClient.makeBytes();
        Thread first = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    for (int i = 0; i < 10; i++) {
                        sendData(20001, bytes);
                    }
                } catch (Exception e) {
                    logger.error("Blew exception", e);
                }
            }
        });
        Thread second = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    for (int i = 0; i < 10; i++) {
                        sendData(20002, bytes);
                    }
                } catch (Exception e) {
                    logger.error("Blew exception", e);
                }
            }
        });
        first.start();
        second.start();
    }

    public static byte[] makeBytes() {
        byte[] bytes = new byte[2 << 20];
        return bytes;
    }

    private static void sendData(final int port, final byte[] bytes) throws SocketException, IOException, InterruptedException {
        long totalBytes;
        try (Socket sock = new Socket("localhost", port)) {
            sock.setTcpNoDelay(true);
            sock.setSoTimeout(2000);
            totalBytes = 0L;
            logger.info("socket established " + sock + " to port " + port + " now waiting 5 seconds to send anything...");
            Thread.sleep(5000L);
            for (int i = 0; i < 1000; i++) {
                sock.getOutputStream().write(bytes);
                totalBytes += bytes.length;
            }
            sock.getOutputStream().flush();
        }
        logger.info("Total bytes sent: " + totalBytes + " to port " + port);
    }

}
