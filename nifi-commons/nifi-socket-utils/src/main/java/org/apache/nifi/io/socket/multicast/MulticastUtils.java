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
package org.apache.nifi.io.socket.multicast;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MulticastSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public final class MulticastUtils {

    private static final Logger logger = new org.apache.nifi.logging.NiFiLog(LoggerFactory.getLogger(MulticastUtils.class));

    public static MulticastSocket createMulticastSocket(final MulticastConfiguration config) throws IOException {
        return createMulticastSocket(0, config);
    }

    public static MulticastSocket createMulticastSocket(final int port, final MulticastConfiguration config) throws IOException {
        if (config == null) {
            throw new IllegalArgumentException("Configuration may not be null.");
        }

        final MulticastSocket socket;
        if (port <= 0) {
            socket = new MulticastSocket();
        } else {
            socket = new MulticastSocket(port);
        }
        socket.setTimeToLive(config.getTtl().getTtl());

        if (config.getSocketTimeout() != null) {
            socket.setSoTimeout(config.getSocketTimeout());
        }

        if (config.getReuseAddress() != null) {
            socket.setReuseAddress(config.getReuseAddress());
        }

        if (config.getReceiveBufferSize() != null) {
            socket.setReceiveBufferSize(config.getReceiveBufferSize());
        }

        if (config.getSendBufferSize() != null) {
            socket.setSendBufferSize(config.getSendBufferSize());
        }

        if (config.getTrafficClass() != null) {
            socket.setTrafficClass(config.getTrafficClass());
        }

        if (config.getLoopbackMode() != null) {
            socket.setLoopbackMode(config.getLoopbackMode());
        }

        return socket;
    }

    public static void closeQuietly(final MulticastSocket socket) {

        if (socket == null) {
            return;
        }

        try {
            socket.close();
        } catch (final Exception ex) {
            logger.debug("Failed to close multicast socket due to: " + ex, ex);
        }

    }

    public static void closeQuietly(final MulticastSocket socket, final InetAddress groupAddress) {

        if (socket == null) {
            return;
        }

        try {
            socket.leaveGroup(groupAddress);
        } catch (final Exception ex) {
            logger.debug("Failed to leave multicast group due to: " + ex, ex);
        }

        try {
            socket.close();
        } catch (final Exception ex) {
            logger.debug("Failed to close multicast socket due to: " + ex, ex);
        }

    }
}
