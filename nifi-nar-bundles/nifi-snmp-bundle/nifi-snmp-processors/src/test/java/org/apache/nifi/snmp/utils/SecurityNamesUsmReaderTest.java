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
package org.apache.nifi.snmp.utils;

import org.junit.jupiter.api.Test;
import org.snmp4j.security.UsmUser;
import org.snmp4j.smi.OctetString;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SecurityNamesUsmReaderTest {

    @Test
    void testReadJson() {
        final List<UsmUser> expectedUsmUsers = new ArrayList<>();
        expectedUsmUsers.add(new UsmUser(
                new OctetString("user1"),
                null,
                null,
                null,
                null
        ));
        expectedUsmUsers.add(new UsmUser(
                new OctetString("user2"),
                null,
                null,
                null,
                null
        ));
        final String securityNames = "user1,user2";
        final UsmReader securityNamesUsmReader = new SecurityNamesUsmReader(securityNames);
        final List<UsmUser> usmUsers = securityNamesUsmReader.readUsm();

        assertEquals(expectedUsmUsers, usmUsers);
    }
}
