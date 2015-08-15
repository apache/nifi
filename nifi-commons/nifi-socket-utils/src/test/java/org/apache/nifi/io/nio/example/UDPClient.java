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

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class UDPClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(UDPClient.class);

    public static void main(final String[] args) throws Exception {
        final byte[] buffer = UDPClient.makeBytes();
        final DatagramPacket packet = new DatagramPacket(buffer, buffer.length, new InetSocketAddress("localhost", 20000));
        final DatagramSocket socket = new DatagramSocket();
        final long startTime = System.nanoTime();
        for (int i = 0; i < 819200; i++) { // 100 MB
            socket.send(packet);
        }
        final long endTime = System.nanoTime();
        final long durationMillis = (endTime - startTime) / 1000000;
        LOGGER.info("Sent all UDP packets without any obvious errors | duration ms= " + durationMillis);
    }

    public static byte[] makeBytes() {
        byte[] bytes = new byte[128];
        return bytes;
    }

}
