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

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import org.slf4j.event.KeyValuePair;

import java.util.List;

/**
 * Filter for the per Process Group logging appender that denies events without the log file suffix key/value pair,
 * so that only component log messages of Process Groups defining a Log File Suffix reach the {@link NifiDiscriminator}
 */
public class LogFileSuffixFilter extends Filter<ILoggingEvent> {
    private static final String KEY = "logFileSuffix";

    @Override
    public FilterReply decide(final ILoggingEvent event) {
        if (!isStarted()) {
            return FilterReply.NEUTRAL;
        }

        final List<KeyValuePair> keyValuePairs = event.getKeyValuePairs();
        if (keyValuePairs != null) {
            for (final KeyValuePair keyValuePair : keyValuePairs) {
                if (KEY.equals(keyValuePair.key)) {
                    return FilterReply.NEUTRAL;
                }
            }
        }
        return FilterReply.DENY;
    }
}
