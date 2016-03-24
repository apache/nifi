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

package org.apache.nifi.controller.cluster;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.zookeeper.common.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperClientConfig {
    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperClientConfig.class);

    private final String connectString;
    private final int sessionTimeoutMillis;
    private final int connectionTimeoutMillis;
    private final String rootPath;

    private ZooKeeperClientConfig(String connectString, int sessionTimeoutMillis, int connectionTimeoutMillis, String rootPath) {
        this.connectString = connectString;
        this.sessionTimeoutMillis = sessionTimeoutMillis;
        this.connectionTimeoutMillis = connectionTimeoutMillis;
        this.rootPath = rootPath.endsWith("/") ? rootPath.substring(0, rootPath.length() - 1) : rootPath;
    }

    public String getConnectString() {
        return connectString;
    }

    public int getSessionTimeoutMillis() {
        return sessionTimeoutMillis;
    }

    public int getConnectionTimeoutMillis() {
        return connectionTimeoutMillis;
    }

    public String getRootPath() {
        return rootPath;
    }

    public String resolvePath(final String path) {
        if (path.startsWith("/")) {
            return rootPath + path;
        }

        return rootPath + "/" + path;
    }

    public static ZooKeeperClientConfig createConfig(final Properties properties) {
        final String connectString = properties.getProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING);
        if (connectString == null || connectString.trim().isEmpty()) {
            throw new IllegalStateException("The '" + NiFiProperties.ZOOKEEPER_CONNECT_STRING + "' property is not set in nifi.properties");
        }

        final long sessionTimeoutMs = getTimePeriod(properties, NiFiProperties.ZOOKEEPER_SESSION_TIMEOUT, NiFiProperties.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT);
        final long connectionTimeoutMs = getTimePeriod(properties, NiFiProperties.ZOOKEEPER_CONNECT_TIMEOUT, NiFiProperties.DEFAULT_ZOOKEEPER_CONNECT_TIMEOUT);
        final String rootPath = properties.getProperty(NiFiProperties.ZOOKEEPER_ROOT_NODE, NiFiProperties.DEFAULT_ZOOKEEPER_ROOT_NODE);

        try {
            PathUtils.validatePath(rootPath);
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException("The '" + NiFiProperties.ZOOKEEPER_ROOT_NODE + "' property in nifi.properties is set to an illegal value: " + rootPath);
        }

        return new ZooKeeperClientConfig(connectString, (int) sessionTimeoutMs, (int) connectionTimeoutMs, rootPath);
    }

    private static int getTimePeriod(final Properties properties, final String propertyName, final String defaultValue) {
        final String timeout = properties.getProperty(propertyName, defaultValue);
        try {
            return (int) FormatUtils.getTimeDuration(timeout, TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            logger.warn("Value of '" + propertyName + "' property is set to '" + timeout + "', which is not a valid time period. Using default of " + defaultValue);
            return (int) FormatUtils.getTimeDuration(defaultValue, TimeUnit.MILLISECONDS);
        }
    }
}
