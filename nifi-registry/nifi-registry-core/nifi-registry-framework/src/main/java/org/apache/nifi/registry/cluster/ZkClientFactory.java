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
package org.apache.nifi.registry.cluster;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

/**
 * Creates and starts the {@link CuratorFramework} client used by
 * {@link ZkLeaderElectionManager} when
 * {@code nifi.registry.cluster.coordination=zookeeper}.
 *
 * <p>The bean is only produced when ZooKeeper coordination is active (checked
 * inside the factory method so we can give a clear error message). It is the
 * caller's responsibility ({@link LeaderElectionConfiguration}) to only
 * request this bean in the appropriate mode.
 */
@Configuration
public class ZkClientFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZkClientFactory.class);

    /**
     * Builds, starts, and returns a {@link CuratorFramework} connected to the
     * ZooKeeper ensemble specified by {@code nifi.registry.zookeeper.connect.string}.
     *
     * <p>The returned client is started here; callers should close it via
     * {@link LeaderElectionConfiguration} / {@link ZkLeaderElectionManager#destroy()}.
     *
     * <p>The bean is declared {@code @Lazy} so that it is only instantiated when
     * first requested. In database-coordination or standalone mode no component
     * requests this bean, so it is never created and a blank connect string
     * does not cause a startup failure.
     */
    @Lazy
    @Bean(destroyMethod = "close")
    public CuratorFramework curatorFramework(final NiFiRegistryProperties properties) {
        final String connectString = properties.getZooKeeperConnectString();
        if (StringUtils.isBlank(connectString)) {
            throw new IllegalStateException(
                    NiFiRegistryProperties.ZOOKEEPER_CONNECT_STRING + " must be set when "
                    + NiFiRegistryProperties.CLUSTER_COORDINATION + "="
                    + NiFiRegistryProperties.CLUSTER_COORDINATION_ZOOKEEPER);
        }

        final int sessionTimeoutMs = (int) properties.getZooKeeperSessionTimeoutMs();
        final int connectTimeoutMs = (int) properties.getZooKeeperConnectTimeoutMs();

        if (properties.isZooKeeperClientSecure()) {
            applyTlsSystemProperties(properties);
        }

        // RetryNTimes(1, 100) mirrors NiFi's createClient() — we prefer fast
        // failure so that startup does not hang if ZooKeeper is unreachable.
        final RetryPolicy retryPolicy = new RetryNTimes(1, 100);

        LOGGER.info("Creating CuratorFramework: connectString={} sessionTimeout={}ms connectTimeout={}ms tls={}",
                connectString, sessionTimeoutMs, connectTimeoutMs, properties.isZooKeeperClientSecure());

        final CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .sessionTimeoutMs(sessionTimeoutMs)
                .connectionTimeoutMs(connectTimeoutMs)
                .retryPolicy(retryPolicy)
                .defaultData(new byte[0])
                .build();

        client.start();
        LOGGER.info("CuratorFramework started.");
        return client;
    }

    /**
     * Configures ZooKeeper TLS by setting the standard {@code zookeeper.ssl.*}
     * and {@code zookeeper.client.secure} system properties before the
     * {@code CuratorFramework} is constructed.
     *
     * <p>ZooKeeper 3.5.5+ reads these properties at client creation time via
     * {@code ZKClientConfig}. Setting them as JVM system properties is the
     * canonical approach used by Apache NiFi.
     *
     * <p>Note: these are process-wide properties. In normal NiFi Registry
     * deployments there is exactly one ZooKeeper client per JVM so this is safe.
     */
    private static void applyTlsSystemProperties(final NiFiRegistryProperties properties) {
        LOGGER.info("Enabling ZooKeeper TLS (zookeeper.client.secure=true).");
        System.setProperty("zookeeper.client.secure", "true");
        // Netty transport is required for TLS; the default NIO transport does not support it.
        System.setProperty("zookeeper.clientCnxnSocket", "org.apache.zookeeper.ClientCnxnSocketNetty");

        final String keystore = properties.getZooKeeperSecurityKeystore();
        if (!StringUtils.isBlank(keystore)) {
            System.setProperty("zookeeper.ssl.keyStore.location", keystore);
        }
        final String keystorePasswd = properties.getZooKeeperSecurityKeystorePasswd();
        if (!StringUtils.isBlank(keystorePasswd)) {
            System.setProperty("zookeeper.ssl.keyStore.password", keystorePasswd);
        }
        final String keystoreType = properties.getZooKeeperSecurityKeystoreType();
        if (!StringUtils.isBlank(keystoreType)) {
            System.setProperty("zookeeper.ssl.keyStore.type", keystoreType);
        }

        final String truststore = properties.getZooKeeperSecurityTruststore();
        if (!StringUtils.isBlank(truststore)) {
            System.setProperty("zookeeper.ssl.trustStore.location", truststore);
        }
        final String truststorePasswd = properties.getZooKeeperSecurityTruststorePasswd();
        if (!StringUtils.isBlank(truststorePasswd)) {
            System.setProperty("zookeeper.ssl.trustStore.password", truststorePasswd);
        }
        final String truststoreType = properties.getZooKeeperSecurityTruststoreType();
        if (!StringUtils.isBlank(truststoreType)) {
            System.setProperty("zookeeper.ssl.trustStore.type", truststoreType);
        }
    }
}
