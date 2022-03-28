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
package org.apache.nifi.stateless.engine;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestStandardStatelessEngine {

    @Test
    public void parseDurationTooSmall() {
        // Falls back to default 1 minute if invalid
        assertEquals(Duration.ofMinutes(1), StandardStatelessEngine.parseDuration("999 ms"));
    }

    @Test
    public void parseDurationInvalid() {
        // Falls back to default 1 minute if invalid
        assertEquals(Duration.ofMinutes(1), StandardStatelessEngine.parseDuration("1 nonsense"));
    }

    @Test
    public void parseDurationValid() {
        assertEquals(Duration.ofSeconds(1), StandardStatelessEngine.parseDuration("1 sec"));
        assertEquals(Duration.ofHours(24), StandardStatelessEngine.parseDuration("24 hours"));
        assertEquals(Duration.ofSeconds(5), StandardStatelessEngine.parseDuration(" 5 secs "));
    }

    @Test
    public void parseDurationNull() {
        assertNull(StandardStatelessEngine.parseDuration(""));
        assertNull(StandardStatelessEngine.parseDuration(" "));
        assertNull(StandardStatelessEngine.parseDuration(null));
    }
}
