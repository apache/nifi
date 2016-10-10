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
package org.apache.nifi.processor;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.apache.nifi.util.FormatUtils;

import org.junit.Test;

public class TestFormatUtils {

    @Test
    public void testParse() {
        assertEquals(3, FormatUtils.getTimeDuration("3000 ms", TimeUnit.SECONDS));
        assertEquals(3000, FormatUtils.getTimeDuration("3000 s", TimeUnit.SECONDS));
        assertEquals(0, FormatUtils.getTimeDuration("999 millis", TimeUnit.SECONDS));
        assertEquals(4L * 24L * 60L * 60L * 1000000000L, FormatUtils.getTimeDuration("4 days", TimeUnit.NANOSECONDS));
        assertEquals(24, FormatUtils.getTimeDuration("1 DAY", TimeUnit.HOURS));
        assertEquals(60, FormatUtils.getTimeDuration("1 hr", TimeUnit.MINUTES));
        assertEquals(60, FormatUtils.getTimeDuration("1 Hrs", TimeUnit.MINUTES));
    }

}
