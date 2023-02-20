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
package org.apache.nifi.dbcp.utils;

public class DataSourceConfiguration {

    private final String url;
    private final String driverName;
    private final String userName;
    private final String password;
    private final long maxWaitMillis;
    private final int maxTotal;
    private final int minIdle;
    private final int maxIdle;
    private final long maxConnLifetimeMillis;
    private final long timeBetweenEvictionRunsMillis;
    private final long minEvictableIdleTimeMillis;
    private final long softMinEvictableIdleTimeMillis;
    private final String validationQuery;

    public String getUrl() {
        return url;
    }

    public String getDriverName() {
        return driverName;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

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

    public DataSourceConfiguration(final Builder builder) {
        this.url = builder.url;
        this.driverName = builder.driverName;
        this.userName = builder.userName;
        this.password = builder.password;
        this.maxWaitMillis = builder.maxWaitMillis;
        this.maxTotal = builder.maxTotal;
        this.minIdle = builder.minIdle;
        this.maxIdle = builder.maxIdle;
        this.maxConnLifetimeMillis = builder.maxConnLifetimeMillis;
        this.timeBetweenEvictionRunsMillis = builder.timeBetweenEvictionRunsMillis;
        this.minEvictableIdleTimeMillis = builder.minEvictableIdleTimeMillis;
        this.softMinEvictableIdleTimeMillis = builder.softMinEvictableIdleTimeMillis;
        this.validationQuery = builder.validationQuery;
    }

    public static class Builder {
        private final String url;
        private final String driverName;
        private final String userName;
        private final String password;
        private long maxWaitMillis = DefaultDataSourceValues.MAX_WAIT_TIME.getLongValue();
        private int maxTotal = DefaultDataSourceValues.MAX_TOTAL_CONNECTIONS.getLongValue().intValue();
        private int minIdle = DefaultDataSourceValues.MIN_IDLE.getLongValue().intValue();
        private int maxIdle = DefaultDataSourceValues.MAX_IDLE.getLongValue().intValue();
        private long maxConnLifetimeMillis = DefaultDataSourceValues.MAX_CONN_LIFETIME.getLongValue();
        private long timeBetweenEvictionRunsMillis = DefaultDataSourceValues.EVICTION_RUN_PERIOD.getLongValue();
        private long minEvictableIdleTimeMillis = DefaultDataSourceValues.MIN_EVICTABLE_IDLE_TIME.getLongValue();
        private long softMinEvictableIdleTimeMillis = DefaultDataSourceValues.SOFT_MIN_EVICTABLE_IDLE_TIME.getLongValue();
        private String validationQuery;

        public Builder(final String url, final String driverName, final String userName, final String password) {
            this.url = url;
            this.driverName = driverName;
            this.userName = userName;
            this.password = password;
        }

        public Builder maxWaitMillis(long maxWaitMillis) {
            this.maxWaitMillis = maxWaitMillis;
            return this;
        }

        public Builder maxTotal(int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        public Builder minIdle(int minIdle) {
            this.minIdle = minIdle;
            return this;
        }

        public Builder maxIdle(int maxIdle) {
            this.maxIdle = maxIdle;
            return this;
        }

        public Builder maxConnLifetimeMillis(long maxConnLifetimeMillis) {
            this.maxConnLifetimeMillis = maxConnLifetimeMillis;
            return this;
        }

        public Builder timeBetweenEvictionRunsMillis(long timeBetweenEvictionRunsMillis) {
            this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
            return this;
        }

        public Builder minEvictableIdleTimeMillis(long minEvictableIdleTimeMillis) {
            this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
            return this;
        }

        public Builder softMinEvictableIdleTimeMillis(long softMinEvictableIdleTimeMillis) {
            this.softMinEvictableIdleTimeMillis = softMinEvictableIdleTimeMillis;
            return this;
        }

        public Builder validationQuery(String validationQuery) {
            this.validationQuery = validationQuery;
            return this;
        }

        public DataSourceConfiguration build() {
            return new DataSourceConfiguration(this);
        }
    }
}