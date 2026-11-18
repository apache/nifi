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
package org.apache.nifi.logging;

import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.spi.FilterReply;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.event.KeyValuePair;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LogFileSuffixFilterTest {
    private static final String LOG_FILE_SUFFIX_KEY = "logFileSuffix";

    private LogFileSuffixFilter filter;

    @BeforeEach
    void setFilter() {
        filter = new LogFileSuffixFilter();
        filter.start();
    }

    @Test
    void testDecideKeyValuePairsNull() {
        final LoggingEvent event = new LoggingEvent();

        assertEquals(FilterReply.DENY, filter.decide(event));
    }

    @Test
    void testDecideKeyValuePairsEmpty() {
        final LoggingEvent event = new LoggingEvent();
        event.setKeyValuePairs(List.of());

        assertEquals(FilterReply.DENY, filter.decide(event));
    }

    @Test
    void testDecideKeyValuePairsWithoutSuffix() {
        final LoggingEvent event = new LoggingEvent();
        event.setKeyValuePairs(List.of(new KeyValuePair("other", "value")));

        assertEquals(FilterReply.DENY, filter.decide(event));
    }

    @Test
    void testDecideKeyValuePairsWithSuffix() {
        final LoggingEvent event = new LoggingEvent();
        event.setKeyValuePairs(List.of(new KeyValuePair("other", "value"), new KeyValuePair(LOG_FILE_SUFFIX_KEY, "suffix")));

        assertEquals(FilterReply.NEUTRAL, filter.decide(event));
    }

    @Test
    void testDecideNotStarted() {
        filter.stop();

        assertEquals(FilterReply.NEUTRAL, filter.decide(new LoggingEvent()));
    }
}
