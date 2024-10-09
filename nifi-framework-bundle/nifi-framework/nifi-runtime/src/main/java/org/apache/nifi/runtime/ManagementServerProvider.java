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
package org.apache.nifi.runtime;

import org.apache.nifi.NiFiServer;

import java.net.InetSocketAddress;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Management Server Provider abstracts loading configuration from System properties provided from Bootstrap Process
 */
class ManagementServerProvider {

    static final String MANAGEMENT_SERVER_ADDRESS = "org.apache.nifi.management.server.address";

    private static final Pattern MANAGEMENT_SERVER_ADDRESS_PATTERN = Pattern.compile("^(.+?):([1-9][0-9]{3,4})$");

    private static final String MANAGEMENT_SERVER_DEFAULT_ADDRESS = "127.0.0.1:52020";

    private static final int ADDRESS_GROUP = 1;

    private static final int PORT_GROUP = 2;

    static ManagementServer getManagementServer(final NiFiServer nifiServer) {
        final String managementServerAddressProperty = System.getProperty(MANAGEMENT_SERVER_ADDRESS, MANAGEMENT_SERVER_DEFAULT_ADDRESS);
        final Matcher matcher = MANAGEMENT_SERVER_ADDRESS_PATTERN.matcher(managementServerAddressProperty);
        if (matcher.matches()) {
            final String addressGroup = matcher.group(ADDRESS_GROUP);
            final String portGroup = matcher.group(PORT_GROUP);
            final int port = Integer.parseInt(portGroup);

            final InetSocketAddress bindAddress = new InetSocketAddress(addressGroup, port);
            return new StandardManagementServer(bindAddress, nifiServer);
        } else {
            throw new IllegalStateException("Management Server Address System Property [%s] not valid [%s]".formatted(MANAGEMENT_SERVER_ADDRESS, managementServerAddressProperty));
        }
    }
}
