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
package org.apache.nifi.web.server;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HostHeaderSanitizationCustomizer implements HttpConfiguration.Customizer {
    private static final Logger logger = LoggerFactory.getLogger(HostHeaderSanitizationCustomizer.class);

    private final String serverName;
    private final int serverPort;
    private final List<String> validHosts;

    /**
     * @param serverName the {@code serverName} to set on the request (the {@code serverPort} will not be set)
     */
    public HostHeaderSanitizationCustomizer(String serverName) {
        this(serverName, 0);
    }

    /**
     * @param serverName the {@code serverName} to set on the request
     * @param serverPort the {@code serverPort} to set on the request
     */
    public HostHeaderSanitizationCustomizer(String serverName, int serverPort) {
        this.serverName = Objects.requireNonNull(serverName);
        this.serverPort = serverPort;

        validHosts = new ArrayList<>();
        validHosts.add(serverName.toLowerCase());
        validHosts.add(serverName.toLowerCase() + ":" + serverPort);
        // Sometimes the hostname is left empty but the port is always populated
        validHosts.add("localhost");
        validHosts.add("localhost:" + serverPort);
        try {
            validHosts.add(InetAddress.getLocalHost().getHostName().toLowerCase());
            validHosts.add(InetAddress.getLocalHost().getHostName().toLowerCase() + ":" + serverPort);
        } catch (final Exception e) {
            logger.warn("Failed to determine local hostname.", e);
        }

        logger.info("Created " + this.toString());
    }

    @Override
    public void customize(Connector connector, HttpConfiguration channelConfig, Request request) {
        final String hostHeader = request.getHeader("Host");
        logger.debug("Received request [" + request.getRequestURI() + "] with host header: " + hostHeader);
        if (!hostHeaderIsValid(hostHeader)) {
            logger.warn("Request host header [" + hostHeader + "] different from web hostname [" +
                    serverName + "(:" + serverPort + ")]. Overriding to [" + serverName + ":" +
                    serverPort + request.getRequestURI() + "]");
            request.setAuthority(serverName, serverPort);
        }
    }

    private boolean hostHeaderIsValid(String hostHeader) {
        return validHosts.contains(hostHeader.toLowerCase().trim());
    }

    @Override
    public String toString() {
        return "HostHeaderSanitizationCustomizer for " + serverName + ":" + serverPort;
    }
}
