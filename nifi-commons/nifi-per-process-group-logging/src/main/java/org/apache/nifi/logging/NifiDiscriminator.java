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
import ch.qos.logback.core.sift.Discriminator;
import org.slf4j.event.KeyValuePair;

public class NifiDiscriminator implements Discriminator<ILoggingEvent> {
    private static final String KEY = "logFileSuffix";

    private boolean started;

    @Override
    public String getDiscriminatingValue(final ILoggingEvent iLoggingEvent) {
        for (KeyValuePair keyValuePair : iLoggingEvent.getKeyValuePairs()) {
            if (keyValuePair.key.equals(getKey())) {
                return keyValuePair.value.toString();
            }
        }
        return null;
    }

    @Override
    public String getKey() {
        return KEY;
    }

    @Override
    public void start() {
        started = true;
    }

    @Override
    public void stop() {
        started = false;
    }

    @Override
    public boolean isStarted() {
        return started;
    }
}
