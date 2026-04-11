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
package org.apache.nifi.web.api.dto.util;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDateTimeAdapter {

    private static final String TEST_DATE_TIME = "2026-01-02T03:04:56Z";

    @Test
    public void testMarshal() throws Exception {
        DateTimeAdapter adapter = new DateTimeAdapter();
        Date date = Date.from(Instant.parse(TEST_DATE_TIME));
        assertEquals(date, adapter.unmarshal(adapter.marshal(date)));
    }

    @Test
    public void testUnmarshal() throws Exception {
        DateTimeAdapter adapter = new DateTimeAdapter();
        String dateStr = adapter.marshal(Date.from(Instant.parse(TEST_DATE_TIME)));
        assertEquals(dateStr, adapter.marshal(adapter.unmarshal(dateStr)));
    }
}
