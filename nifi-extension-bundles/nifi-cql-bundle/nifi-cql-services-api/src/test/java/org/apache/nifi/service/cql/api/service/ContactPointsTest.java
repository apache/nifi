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
package org.apache.nifi.service.cql.api.service;

import org.apache.nifi.service.cql.api.service.ContactPoints.HostPort;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ContactPointsTest {

    @ParameterizedTest
    @CsvSource({
            // hostname and IPv4, with and without a port
            "node1:9042,            node1,          9042",
            "node1,                 node1,          9042",
            "10.0.0.5:19042,        10.0.0.5,       19042",
            "10.0.0.5,              10.0.0.5,       9042",
            // bracketed IPv6, with and without a port
            "[::1]:19042,           ::1,            19042",
            "[::1],                 ::1,            9042",
            "[2001:db8::5]:9042,    2001:db8::5,    9042",
            // unbracketed IPv6: the whole entry is the host, so no port can be named
            "::1,                   ::1,            9042",
            "2001:db8::5,           2001:db8::5,    9042",
            // surrounding whitespace is insignificant
            "'  node1 : 9042 ',     node1,          9042",
    })
    void parsesOneEntry(final String entry, final String expectedHost, final int expectedPort) {
        assertEquals(List.of(new HostPort(expectedHost, expectedPort)), ContactPoints.parse(entry));
    }

    @Test
    void parsesSeveralEntriesInOrder() {
        assertEquals(
                List.of(new HostPort("node1", 9042), new HostPort("node2", 19042), new HostPort("::1", 9042)),
                ContactPoints.parse("node1, node2:19042, [::1]"));
    }

    /**
     * Every rejection names the offending entry, since a multi-entry value would otherwise leave the user
     * guessing which part was wrong.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "node1:abc",        // non-numeric port
            "node1:",           // empty port
            "node1:0",          // port below range
            "node1:65536",      // port above range
            ":9042",            // no host
            "[::1",             // unterminated bracket
            "[::1]x",           // trailing text after the bracket
    })
    void rejectsMalformedEntryNamingIt(final String entry) {
        final IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> ContactPoints.parse("good:9042," + entry));
        assertTrue(e.getMessage().contains(entry.trim()),
                () -> "expected message to name the entry '" + entry + "' but was: " + e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   "})
    void rejectsBlankValue(final String value) {
        assertThrows(IllegalArgumentException.class, () -> ContactPoints.parse(value));
    }

    @Test
    void rejectsNullValue() {
        assertThrows(IllegalArgumentException.class, () -> ContactPoints.parse(null));
    }
}
