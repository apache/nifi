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

import java.util.concurrent.TimeUnit;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.nifi.util.FormatUtils;

public enum DefaultDataSourceValues {

    MAX_WAIT_TIME("500 millis") {
        @Override
        public Long getLongValue() {
            return (long) FormatUtils.getPreciseTimeDuration(MAX_WAIT_TIME.value, TimeUnit.MILLISECONDS);
        }
    },
    MAX_TOTAL_CONNECTIONS("8"),

    /**
     * Copied from {@link GenericObjectPoolConfig#DEFAULT_MIN_IDLE} in Commons-DBCP 2.7.0
     */
    MIN_IDLE("0"),

    /**
     * Copied from {@link GenericObjectPoolConfig#DEFAULT_MAX_IDLE} in Commons-DBCP 2.7.0
     */
    MAX_IDLE("8"),

    MAX_CONN_LIFETIME("-1"),

    EVICTION_RUN_PERIOD("-1"),

    MIN_EVICTABLE_IDLE_TIME("30 mins") {
        @Override
        public Long getLongValue() {
            return (long) FormatUtils.getPreciseTimeDuration(MAX_WAIT_TIME.value, TimeUnit.MINUTES);
        }
    },

    SOFT_MIN_EVICTABLE_IDLE_TIME("-1");


    private final String value;

    DefaultDataSourceValues(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public Long getLongValue() {
        return Long.parseLong(value);
    }
}
