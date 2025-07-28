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
package org.apache.nifi.framework.cluster.zookeeper;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.zookeeper.common.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ZooKeeperClientConfig {

    public static final String NETTY_CLIENT_CNXN_SOCKET =
        org.apache.zookeeper.ClientCnxnSocketNetty.class.getName();

    public static final String NIO_CLIENT_CNXN_SOCKET =
        org.apache.zookeeper.ClientCnxnSocketNIO.class.getName();

    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperClientConfig.class);
    private static final Pattern PORT_PATTERN = Pattern.compile("[0-9]{1,5}");

    private final String connectString;
    private final int sessionTimeoutMillis;
    private final int connectionTimeoutMillis;
    private final String rootPath;
    private final boolean clientSecure;
    private final boolean withEnsembleTracker;
    private final String keyStore;
    private final String keyStoreType;
    private final String keyStorePassword;
    private final String trustStore;
    private final String trustStoreType;
    private final String trustStorePassword;
    private final String authType;
    private final String authPrincipal;
    private final String removeHostFromPrincipal;
    private final String removeRealmFromPrincipal;
    private final int juteMaxbuffer;

    private ZooKeeperClientConfig(String connectString, int sessionTimeoutMillis, int connectionTimeoutMillis,
                                  String rootPath, String authType, String authPrincipal, String removeHostFromPrincipal,
                                  String removeRealmFromPrincipal, boolean clientSecure, String keyStore, String keyStoreType,
                                  String keyStorePassword, String trustStore, String trustStoreType, String trustStorePassword,
                                  final int juteMaxbuffer, boolean withEnsembleTracker) {
        this.connectString = connectString;
        this.sessionTimeoutMillis = sessionTimeoutMillis;
        this.connectionTimeoutMillis = connectionTimeoutMillis;
        this.rootPath = rootPath.endsWith("/") ? rootPath.substring(0, rootPath.length() - 1) : rootPath;
        this.clientSecure = clientSecure;
        this.keyStore = keyStore;
        this.keyStoreType = keyStoreType;
        this.keyStorePassword = keyStorePassword;
        this.trustStore = trustStore;
        this.trustStoreType = trustStoreType;
        this.trustStorePassword = trustStorePassword;
        this.authType = authType;
        this.authPrincipal = authPrincipal;
        this.removeHostFromPrincipal = removeHostFromPrincipal;
        this.removeRealmFromPrincipal = removeRealmFromPrincipal;
        this.juteMaxbuffer = juteMaxbuffer;
        this.withEnsembleTracker = withEnsembleTracker;
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

    public boolean isClientSecure() {
        return clientSecure;
    }

    public boolean isWithEnsembleTracker() {
        return withEnsembleTracker;
    }

    public String getConnectionSocket() {
        return (isClientSecure() ? NETTY_CLIENT_CNXN_SOCKET : NIO_CLIENT_CNXN_SOCKET);
    }

    public String getKeyStore() {
        return keyStore;
    }

    public String getKeyStoreType() {
        return keyStoreType;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public String getTrustStore() {
        return trustStore;
    }

    public String getTrustStoreType() {
        return trustStoreType;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public String getAuthType() {
        return authType;
    }

    public String getAuthPrincipal() {
        return authPrincipal;
    }

    public String getRemoveHostFromPrincipal() {
        return removeHostFromPrincipal;
    }

    public String getRemoveRealmFromPrincipal() {
        return removeRealmFromPrincipal;
    }

    public int getJuteMaxbuffer() {
        return juteMaxbuffer;
    }

    public String resolvePath(final String path) {
        if (path.startsWith("/")) {
            return rootPath + path;
        }

        return rootPath + "/" + path;
    }

    public static ZooKeeperClientConfig createConfig(final NiFiProperties nifiProperties) {
        final String connectString = nifiProperties.getProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING);
        if (connectString == null || connectString.isBlank()) {
            throw new IllegalStateException("The '" + NiFiProperties.ZOOKEEPER_CONNECT_STRING + "' property is not set in nifi.properties");
        }
        final String cleanedConnectString = cleanConnectString(connectString);
        if (cleanedConnectString.isEmpty()) {
            throw new IllegalStateException("The '" + NiFiProperties.ZOOKEEPER_CONNECT_STRING +
                    "' property is set in nifi.properties but needs to be in pairs of host:port separated by commas");
        }
        final long sessionTimeoutMs = getTimePeriod(nifiProperties, NiFiProperties.ZOOKEEPER_SESSION_TIMEOUT, NiFiProperties.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT);
        final long connectionTimeoutMs = getTimePeriod(nifiProperties, NiFiProperties.ZOOKEEPER_CONNECT_TIMEOUT, NiFiProperties.DEFAULT_ZOOKEEPER_CONNECT_TIMEOUT);
        final String rootPath = nifiProperties.getProperty(NiFiProperties.ZOOKEEPER_ROOT_NODE, NiFiProperties.DEFAULT_ZOOKEEPER_ROOT_NODE);
        final boolean clientSecure = nifiProperties.isZooKeeperClientSecure();
        final boolean withEnsembleTracker = nifiProperties.isZookeeperClientWithEnsembleTracker();
        final String keyStore = getPreferredProperty(nifiProperties, NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE, NiFiProperties.SECURITY_KEYSTORE);
        final String keyStoreType = StringUtils.stripToNull(getPreferredProperty(nifiProperties, NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_TYPE, NiFiProperties.SECURITY_KEYSTORE_TYPE));
        final String keyStorePassword = getPreferredProperty(nifiProperties, NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_PASSWD, NiFiProperties.SECURITY_KEYSTORE_PASSWD);
        final String trustStore = getPreferredProperty(nifiProperties, NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE, NiFiProperties.SECURITY_TRUSTSTORE);
        final String trustStoreType = StringUtils.stripToNull(getPreferredProperty(nifiProperties, NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_TYPE, NiFiProperties.SECURITY_TRUSTSTORE_TYPE));
        final String trustStorePassword = getPreferredProperty(nifiProperties, NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_PASSWD, NiFiProperties.SECURITY_TRUSTSTORE_PASSWD);
        final String authType = nifiProperties.getProperty(NiFiProperties.ZOOKEEPER_AUTH_TYPE, NiFiProperties.DEFAULT_ZOOKEEPER_AUTH_TYPE);
        final String authPrincipal = nifiProperties.getKerberosServicePrincipal();
        final String removeHostFromPrincipal = nifiProperties.getProperty(NiFiProperties.ZOOKEEPER_KERBEROS_REMOVE_HOST_FROM_PRINCIPAL,
                NiFiProperties.DEFAULT_ZOOKEEPER_KERBEROS_REMOVE_HOST_FROM_PRINCIPAL);
        final String removeRealmFromPrincipal = nifiProperties.getProperty(NiFiProperties.ZOOKEEPER_KERBEROS_REMOVE_REALM_FROM_PRINCIPAL,
                NiFiProperties.DEFAULT_ZOOKEEPER_KERBEROS_REMOVE_REALM_FROM_PRINCIPAL);
        final int juteMaxbuffer = nifiProperties.getIntegerProperty(NiFiProperties.ZOOKEEPER_JUTE_MAXBUFFER, NiFiProperties.DEFAULT_ZOOKEEPER_JUTE_MAXBUFFER);

        try {
            PathUtils.validatePath(rootPath);
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException("The '" + NiFiProperties.ZOOKEEPER_ROOT_NODE + "' property in nifi.properties is set to an illegal value: " + rootPath);
        }

        return new ZooKeeperClientConfig(
            cleanedConnectString,
            (int) sessionTimeoutMs,
            (int) connectionTimeoutMs,
            rootPath,
            authType,
            authPrincipal,
            removeHostFromPrincipal,
            removeRealmFromPrincipal,
            clientSecure,
            keyStore,
            keyStoreType,
            keyStorePassword,
            trustStore,
            trustStoreType,
            trustStorePassword,
            juteMaxbuffer,
            withEnsembleTracker
        );
    }

    private static int getTimePeriod(final NiFiProperties nifiProperties, final String propertyName, final String defaultValue) {
        final String timeout = nifiProperties.getProperty(propertyName, defaultValue);
        try {
            return (int) FormatUtils.getTimeDuration(timeout, TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            logger.warn("Value of '{}' property is set to '{}', which is not a valid time period. Using default of {}", propertyName, timeout, defaultValue);
            return (int) FormatUtils.getTimeDuration(defaultValue, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Retrieves the preferred property value if it's set, otherwise retrieves the default property. If neither are set, returns null.
     * @param properties The NiFi properties configuration.
     * @param preferredPropertyName The preferred property to get from NiFi properties.
     * @param defaultPropertyName The backup property to get from NiFi properties if the preferred property is not present.
     * @return Returns the property in order of preference.
     */
    public static String getPreferredProperty(final NiFiProperties properties, final String preferredPropertyName, final String defaultPropertyName) {
        String retrievedProperty = properties.getProperty(preferredPropertyName);

        if (StringUtils.isBlank(retrievedProperty)) {
            retrievedProperty = properties.getProperty(defaultPropertyName);
        }

        return retrievedProperty;
    }

    /**
     * Takes a given connect string and splits it by ',' character. For each
     * split result trims whitespace then splits by ':' character. For each
     * secondary split if a single value is returned it is trimmed and then the
     * default zookeeper 2181 is append by adding ":2181". If two values are
     * returned then the second value is evaluated to ensure it contains only
     * digits and if not then the entry is in error and exception is raised.
     * If more than two values are
     * returned the entry is in error and an exception is raised.
     * Each entry is trimmed and if empty the
     * entry is skipped. After all splits are cleaned then they are all appended
     * back together demarcated by "," and the full string is returned.
     *
     * @param connectString the string to clean
     * @return cleaned connect string guaranteed to be non null but could be
     * empty
     * @throws IllegalStateException if any portions could not be cleaned/parsed
     */
    public static String cleanConnectString(final String connectString) {
        final String nospaces = StringUtils.deleteWhitespace(connectString);
        final String hostPortPairs[] = StringUtils.split(nospaces, ",", 100);
        final List<String> cleanedEntries = new ArrayList<>(hostPortPairs.length);
        for (final String pair : hostPortPairs) {
            final String pairSplits[] = StringUtils.split(pair, ":", 3);
            if (pairSplits.length > 2 || pairSplits[0].isEmpty()) {
                throw new IllegalStateException("Invalid host:port pair entry '" +
                        pair + "' in nifi.properties " + NiFiProperties.ZOOKEEPER_CONNECT_STRING + "' property");
            }
            if (pairSplits.length == 1) {
                cleanedEntries.add(pairSplits[0] + ":2181");
            } else {
                if (PORT_PATTERN.matcher(pairSplits[1]).matches()) {
                    cleanedEntries.add(pairSplits[0] + ":" + pairSplits[1]);
                } else {
                throw new IllegalStateException("The port specified in this pair must be 1 to 5 digits only but was '" +
                        pair + "' in nifi.properties " + NiFiProperties.ZOOKEEPER_CONNECT_STRING + "' property");
                }
            }
        }
        return StringUtils.join(cleanedEntries, ",");
    }
}
