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

import org.apache.nifi.util.FormatUtils;
import org.junit.jupiter.api.Test;

import java.sql.Driver;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.dbcp.BasicDataSourceConfiguration.DEFAULT_MAX_CONN_LIFETIME;
import static org.apache.nifi.dbcp.BasicDataSourceConfiguration.DEFAULT_MAX_IDLE;
import static org.apache.nifi.dbcp.BasicDataSourceConfiguration.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MINS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class BasicDataSourceBuilderConfigurationTest {

    private static final String URL = "url";
    private static final Driver DRIVER = mock(Driver.class);
    private static final String USER_NAME = "userName";
    private static final String PASSWORD = "password";

    @Test
    void testDefaultValues() {
        BasicDataSourceConfiguration configuration = new BasicDataSourceConfiguration.Builder(URL, DRIVER, USER_NAME, PASSWORD)
                .build();

        assertEquals(URL, configuration.getUrl());
        assertEquals(DRIVER, configuration.getDriver());
        assertEquals(USER_NAME, configuration.getUserName());
        assertEquals(PASSWORD, configuration.getPassword());

        assertEquals(Long.parseLong(DEFAULT_MAX_CONN_LIFETIME), configuration.getMaxConnLifetimeMillis());
        assertEquals(Integer.parseInt(DEFAULT_MAX_IDLE), configuration.getMaxIdle());
        assertEquals((long) FormatUtils.getPreciseTimeDuration(DEFAULT_MIN_EVICTABLE_IDLE_TIME_MINS, TimeUnit.MINUTES), configuration.getMinEvictableIdleTimeMillis());
    }

}
