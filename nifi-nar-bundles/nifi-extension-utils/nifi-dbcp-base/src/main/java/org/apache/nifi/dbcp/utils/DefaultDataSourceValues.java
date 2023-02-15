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

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public enum DefaultDataSourceValues {


    MAX_WAIT_MILLIS("500", "500 millis"),

    MAX_TOTAL_CONNECTIONS("8", "8"),
    /**
     * Copied from {@link GenericObjectPoolConfig#DEFAULT_MIN_IDLE} in Commons-DBCP 2.7.0
     */
    MIN_IDLE("0", "0"),
    /**
     * Copied from {@link GenericObjectPoolConfig#DEFAULT_MAX_IDLE} in Commons-DBCP 2.7.0
     */
    MAX_IDLE("8", "8"),
    /**
     * Copied from private variable {@link BasicDataSource#maxConnLifetimeMillis} in Commons-DBCP 2.7.0
     */
    MAX_CONN_LIFETIME("-1", "-1"),
    /**
     * Copied from {@link GenericObjectPoolConfig#DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS} in Commons-DBCP 2.7.0
     */
    EVICTION_RUN_PERIOD("-1", "-1"),
    /**
     * Copied from {@link GenericObjectPoolConfig#DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS} in Commons-DBCP 2.7.0
     * and converted from 1800000L to "1800000 millis" to "30 mins"
     */
    MIN_EVICTABLE_IDLE_TIME_MILLIS("1800000", "30 mins"),
    /**
     * Copied from {@link GenericObjectPoolConfig#DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS} in Commons-DBCP 2.7.0
     */
    SOFT_MIN_EVICTABLE_IDLE_TIME("-1", "-1");


    private final String value;
    private final String propertyValue;

    DefaultDataSourceValues(String value, String propertyValue) {
        this.value = value;
        this.propertyValue = propertyValue;
    }

    public int getIntValue() {
        return Integer.parseInt(value);
    }

    public long getLongValue() {
        return Long.parseLong(value);
    }

    public String getPropertyValue() {
        return propertyValue;
    }
}
