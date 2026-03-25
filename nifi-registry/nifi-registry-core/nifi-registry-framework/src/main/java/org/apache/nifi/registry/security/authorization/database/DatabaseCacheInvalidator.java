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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * {@link CacheInvalidator} backed by the {@code CACHE_VERSION} database table.
 *
 * <p>{@link #notifyChanged} increments the version counter for the given
 * domain.  Other nodes discover the change through the {@link CacheRefreshPoller}
 * polling loop, so {@link #watchDomain} is a no-op here.
 *
 * <p>Used when {@code nifi.registry.cluster.coordination=database} (the default).
 */
public class DatabaseCacheInvalidator implements CacheInvalidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseCacheInvalidator.class);

    private final JdbcTemplate jdbcTemplate;

    public DatabaseCacheInvalidator(final DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public void notifyChanged(final String domain) {
        try {
            jdbcTemplate.update(
                    "UPDATE CACHE_VERSION SET VERSION = VERSION + 1 WHERE CACHE_DOMAIN = ?",
                    domain);
        } catch (final Exception e) {
            LOGGER.warn("Failed to bump CACHE_VERSION for domain {}: {}", domain, e.getMessage());
        }
    }

    /**
     * No-op: {@link CacheRefreshPoller} drives cache refreshes directly in
     * database-coordination mode.
     */
    @Override
    public void watchDomain(final String domain, final Runnable onChanged) {
        // intentionally empty — polling handled by CacheRefreshPoller
    }
}
