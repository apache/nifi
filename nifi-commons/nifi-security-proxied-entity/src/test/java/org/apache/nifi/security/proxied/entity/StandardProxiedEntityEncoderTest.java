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
package org.apache.nifi.security.proxied.entity;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StandardProxiedEntityEncoderTest {

    @ParameterizedTest
    @CsvSource(
            delimiter = ':',
            value = {
                    ":<null>",
                    "'':<>",
                    "username:<username>",
                    "CN=Common Name, OU=Organizational Unit:<CN=Common Name, OU=Organizational Unit>",
                    "CN=nifi.apache.org, O=Organization:<CN=nifi.apache.org, O=Organization>",
                    "<username>:<\\<username\\>>",
                    "<<username>>:<\\<\\<username\\>\\>>",
                    "CN=\uD83D\uDE00:<<Q0498J+YgA==>>"
            }
    )
    void testGetEncodedEntity(final String identity, final String expected) {
        final String encodedEntity = StandardProxiedEntityEncoder.getInstance().getEncodedEntity(identity);

        assertEquals(expected, encodedEntity);
    }
}
