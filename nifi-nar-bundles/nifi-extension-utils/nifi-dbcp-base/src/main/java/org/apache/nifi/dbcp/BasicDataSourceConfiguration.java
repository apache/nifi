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
package org.apache.nifi.dbcp;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.util.FormatUtils;

import java.sql.Driver;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BasicDataSourceConfiguration {

    /**
     * Copied from {@link GenericObjectPoolConfig#DEFAULT_MIN_IDLE} in Commons-DBCP 2.7.0
     */
    static final String DEFAULT_MIN_IDLE = "0";
    /**
     * Copied from {@link GenericObjectPoolConfig#DEFAULT_MAX_IDLE} in Commons-DBCP 2.7.0
     */
    static final String DEFAULT_MAX_TOTAL_CONNECTIONS = "8";

    static final String DEFAULT_MAX_IDLE = DEFAULT_MAX_TOTAL_CONNECTIONS;
    /**
     * Copied from private variable {@link BasicDataSource#maxConnLifetimeMillis} in Commons-DBCP 2.7.0
     */
    static final String DEFAULT_MAX_CONN_LIFETIME = "-1";
    /**
     * Copied from {@link GenericObjectPoolConfig#DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS} in Commons-DBCP 2.7.0
     */
    static final String DEFAULT_EVICTION_RUN_PERIOD = String.valueOf(-1L);
    /**
     * Copied from {@link GenericObjectPoolConfig#DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS} in Commons-DBCP 2.7.0
     * and converted from 1800000L to "1800000 millis" to "30 mins"
     */
    static final String DEFAULT_MIN_EVICTABLE_IDLE_TIME_MINS = "30 mins";
    /**
     * Copied from {@link GenericObjectPoolConfig#DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS} in Commons-DBCP 2.7.0
     */
    static final String DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME = String.valueOf(-1L);
    static final String DEFAULT_MAX_WAIT_MILLIS = "500 millis";

    private long maxWaitMillis;
    private int maxTotal;
    private int minIdle;
    private int maxIdle;
    private long maxConnLifetimeMillis;
    private long timeBetweenEvictionRunsMillis;
    private long minEvictableIdleTimeMillis;
    private long softMinEvictableIdleTimeMillis;
    private String validationQuery;
    private String url;
    private Driver driver;

    public long getMaxWaitMillis() {
        return maxWaitMillis;
    }

    public int getMaxTotal() {
        return maxTotal;
    }

    public int getMinIdle() {
        return minIdle;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public long getMaxConnLifetimeMillis() {
        return maxConnLifetimeMillis;
    }

    public long getTimeBetweenEvictionRunsMillis() {
        return timeBetweenEvictionRunsMillis;
    }

    public long getMinEvictableIdleTimeMillis() {
        return minEvictableIdleTimeMillis;
    }

    public long getSoftMinEvictableIdleTimeMillis() {
        return softMinEvictableIdleTimeMillis;
    }

    public String getValidationQuery() {
        return validationQuery;
    }

    public String getUrl() {
        return url;
    }

    public Driver getDriver() {
        return driver;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public List<PropertyDescriptor> getDynamicProperties() {
        return dynamicProperties;
    }

    private String userName;
    private String password;
    private List<PropertyDescriptor> dynamicProperties;

    public BasicDataSourceConfiguration(long maxWaitMillis, int maxTotal, int minIdle, int maxIdle, long maxConnLifetimeMillis, long timeBetweenEvictionRunsMillis,
                                        long minEvictableIdleTimeMillis, long softMinEvictableIdleTimeMillis, String validationQuery, String url, Driver driver,
                                        String userName, String password, List<PropertyDescriptor> dynamicProperties) {
        this.maxWaitMillis = maxWaitMillis;
        this.maxTotal = maxTotal;
        this.minIdle = minIdle;
        this.maxIdle = maxIdle;
        this.maxConnLifetimeMillis = maxConnLifetimeMillis;
        this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
        this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
        this.softMinEvictableIdleTimeMillis = softMinEvictableIdleTimeMillis;
        this.validationQuery = validationQuery;
        this.url = url;
        this.driver = driver;
        this.userName = userName;
        this.password = password;
        this.dynamicProperties = dynamicProperties;
    }

    public static class Builder {
        private long maxWaitMillis = (long) FormatUtils.getPreciseTimeDuration(DEFAULT_MAX_WAIT_MILLIS, TimeUnit.MILLISECONDS);
        private int maxTotal = Integer.parseInt(DEFAULT_MAX_TOTAL_CONNECTIONS);
        private int minIdle = Integer.parseInt(DEFAULT_MIN_IDLE);
        private int maxIdle = Integer.parseInt(DEFAULT_MAX_IDLE);
        private long maxConnLifetimeMillis = Long.parseLong(DEFAULT_MAX_CONN_LIFETIME);
        private long timeBetweenEvictionRunsMillis = Long.parseLong(DEFAULT_EVICTION_RUN_PERIOD);
        private long minEvictableIdleTimeMillis = (long) FormatUtils.getPreciseTimeDuration(DEFAULT_MIN_EVICTABLE_IDLE_TIME_MINS, TimeUnit.MINUTES);
        private long softMinEvictableIdleTimeMillis = Long.parseLong(DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME);
        private String validationQuery;
        private List<PropertyDescriptor> dynamicProperties;
        private final String url;
        private final Driver driver;
        private final String userName;
        private final String password;

        public Builder(final String url, final Driver driver, final String userName, final String password) {
            this.url = url;
            this.driver = driver;
            this.userName = userName;
            this.password = password;
        }

        public Builder maxWaitMillis(PropertyValue maxWaitMillis) {
            if (maxWaitMillis.evaluateAttributeExpressions().isSet()) {
                this.maxWaitMillis = extractMillisWithInfinite(maxWaitMillis.evaluateAttributeExpressions());
            }
            return this;
        }

        public Builder maxTotal(PropertyValue maxTotal) {
            if (maxTotal.evaluateAttributeExpressions().isSet()) {
                this.maxTotal = maxTotal.evaluateAttributeExpressions().asInteger();
            }
            return this;
        }

        public Builder minIdle(PropertyValue minIdle) {
            if (minIdle.evaluateAttributeExpressions().isSet()) {
                this.minIdle = minIdle.evaluateAttributeExpressions().asInteger();
            }
            return this;
        }

        public Builder maxIdle(PropertyValue maxIdle) {
            if (maxIdle.evaluateAttributeExpressions().isSet()) {
                this.maxIdle = maxIdle.evaluateAttributeExpressions().asInteger();
            }
            return this;
        }

        public Builder maxConnLifetimeMillis(PropertyValue maxConnLifetimeMillis) {
            if (maxConnLifetimeMillis.evaluateAttributeExpressions().isSet()) {
                this.maxConnLifetimeMillis = extractMillisWithInfinite(maxConnLifetimeMillis.evaluateAttributeExpressions());
            }
            return this;
        }

        public Builder timeBetweenEvictionRunsMillis(PropertyValue timeBetweenEvictionRunsMillis) {
            if (timeBetweenEvictionRunsMillis.evaluateAttributeExpressions().isSet()) {
                this.timeBetweenEvictionRunsMillis = extractMillisWithInfinite(timeBetweenEvictionRunsMillis.evaluateAttributeExpressions());
            }
            return this;
        }

        public Builder minEvictableIdleTimeMillis(PropertyValue minEvictableIdleTimeMillis) {
            if (minEvictableIdleTimeMillis.evaluateAttributeExpressions().isSet()) {
                this.minEvictableIdleTimeMillis = extractMillisWithInfinite(minEvictableIdleTimeMillis.evaluateAttributeExpressions());
            }
            return this;
        }

        public Builder softMinEvictableIdleTimeMillis(PropertyValue softMinEvictableIdleTimeMillis) {
            if (softMinEvictableIdleTimeMillis.evaluateAttributeExpressions().isSet()) {
                this.softMinEvictableIdleTimeMillis = extractMillisWithInfinite(softMinEvictableIdleTimeMillis.evaluateAttributeExpressions());
            }
            return this;
        }

        public Builder validationQuery(String validationQuery) {
            this.validationQuery = validationQuery;
            return this;
        }

        public Builder dynamicProperties(List<PropertyDescriptor> dynamicProperties) {
            this.dynamicProperties = dynamicProperties;
            return this;
        }

        public BasicDataSourceConfiguration build() {
            return new BasicDataSourceConfiguration(maxWaitMillis, maxTotal, minIdle, maxIdle, maxConnLifetimeMillis, timeBetweenEvictionRunsMillis,
                    minEvictableIdleTimeMillis, softMinEvictableIdleTimeMillis, validationQuery, url, driver, userName, password, dynamicProperties);
        }
    }

    static Long extractMillisWithInfinite(PropertyValue prop) {
        return "-1".equals(prop.getValue()) ? -1 : prop.asTimePeriod(TimeUnit.MILLISECONDS);
    }
}
