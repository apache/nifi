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
package org.apache.nifi.registry.security.authorization.database;

import jakarta.annotation.PostConstruct;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;

/**
 * Background component that polls the {@code CACHE_VERSION} table and triggers
 * in-memory cache reloads on {@link DatabaseAccessPolicyProvider} and
 * {@link DatabaseUserGroupProvider} when another cluster node has made changes.
 *
 * <p>Only active when {@code nifi.registry.cluster.enabled=true} in
 * {@code nifi-registry.properties}. The poll interval defaults to 15 s and is
 * configurable via {@code nifi.registry.cluster.cache.refresh.interval.ms}.
 *
 * <p>Provider instances register themselves by calling
 * {@link #setAccessPolicyProvider} / {@link #setUserGroupProvider} on the
 * poller instance, which is injected into each provider via the
 * {@code @AuthorizerContext} mechanism during provider initialisation.
 */
@Component
public class CacheRefreshPoller implements DisposableBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheRefreshPoller.class);

    static final String DOMAIN_ACCESS_POLICIES = "ACCESS_POLICIES";
    static final String DOMAIN_USER_GROUPS = "USER_GROUPS";

    // Provider instances are not Spring beans; they register themselves on initialisation
    // via the CacheRefreshPoller instance injected through @AuthorizerContext.
    private DatabaseAccessPolicyProvider accessPolicyProvider;
    private DatabaseUserGroupProvider userGroupProvider;

    private final JdbcTemplate jdbcTemplate;
    private final NiFiRegistryProperties properties;
    private final ScheduledExecutorService scheduler;

    // -1 means "first poll — record version but do not refresh"
    private final AtomicLong lastAccessPoliciesVersion = new AtomicLong(-1);
    private final AtomicLong lastUserGroupsVersion = new AtomicLong(-1);

    @Autowired
    public CacheRefreshPoller(final DataSource dataSource, final NiFiRegistryProperties properties) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.properties = properties;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread t = new Thread(r, "CacheRefreshPoller");
            t.setDaemon(true);
            return t;
        });
    }

    @PostConstruct
    public void start() {
        if (!properties.isClusterEnabled()) {
            LOGGER.info("Cluster mode is disabled; CacheRefreshPoller will not start.");
            return;
        }

        if (properties.isZooKeeperCoordination()) {
            LOGGER.info("Cluster coordination=zookeeper; CacheRefreshPoller is replaced by ZkCacheInvalidator watches.");
            return;
        }

        final long intervalMs = properties.getClusterCacheRefreshIntervalMs();
        LOGGER.info("Starting CacheRefreshPoller with interval {} ms.", intervalMs);
        scheduler.scheduleWithFixedDelay(this::pollAndRefresh, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void destroy() {
        LOGGER.info("Shutting down CacheRefreshPoller.");
        scheduler.shutdownNow();
    }

    /**
     * Called by {@link DatabaseAccessPolicyProvider} during initialisation so the
     * poller can trigger a cache reload on that provider when the version changes.
     */
    void setAccessPolicyProvider(final DatabaseAccessPolicyProvider provider) {
        accessPolicyProvider = provider;
    }

    /**
     * Called by {@link DatabaseUserGroupProvider} during initialisation so the
     * poller can trigger a cache reload on that provider when the version changes.
     */
    void setUserGroupProvider(final DatabaseUserGroupProvider provider) {
        userGroupProvider = provider;
    }

    private void pollAndRefresh() {
        try {
            checkDomainAndRefreshIfNeeded(
                    DOMAIN_ACCESS_POLICIES,
                    lastAccessPoliciesVersion,
                    accessPolicyProvider);
            checkDomainAndRefreshIfNeeded(
                    DOMAIN_USER_GROUPS,
                    lastUserGroupsVersion,
                    userGroupProvider);
        } catch (final Exception e) {
            LOGGER.error("Unexpected error during cache version poll", e);
        }
    }

    private void checkDomainAndRefreshIfNeeded(final String domain,
            final AtomicLong lastKnown,
            final Object provider) {
        try {
            final Long currentVersion = jdbcTemplate.queryForObject(
                    "SELECT VERSION FROM CACHE_VERSION WHERE CACHE_DOMAIN = ?",
                    Long.class, domain);

            if (currentVersion == null) {
                // Table exists but domain not seeded yet — non-cluster or mid-migration; skip.
                return;
            }

            final long prev = lastKnown.get();
            if (prev == -1L) {
                // First poll: record baseline version without triggering a refresh
                // (this node's provider already loaded the current data at startup).
                lastKnown.set(currentVersion);
                LOGGER.debug("CacheRefreshPoller baseline for domain {}: version={}", domain, currentVersion);
                return;
            }

            if (currentVersion > prev) {
                LOGGER.debug("Cache version changed for domain {} ({} → {}); refreshing.",
                        domain, prev, currentVersion);
                runRefresh(domain, provider);
                lastKnown.set(currentVersion);
            }
        } catch (final Exception e) {
            LOGGER.warn("Failed to read CACHE_VERSION for domain {}: {}", domain, e.getMessage());
        }
    }

    private void runRefresh(final String domain, final Object provider) {
        if (DOMAIN_ACCESS_POLICIES.equals(domain) && provider instanceof DatabaseAccessPolicyProvider p) {
            p.refreshAccessPolicyHolder();
        } else if (DOMAIN_USER_GROUPS.equals(domain) && provider instanceof DatabaseUserGroupProvider p) {
            p.refreshUserGroupHolder();
        }
    }
}
