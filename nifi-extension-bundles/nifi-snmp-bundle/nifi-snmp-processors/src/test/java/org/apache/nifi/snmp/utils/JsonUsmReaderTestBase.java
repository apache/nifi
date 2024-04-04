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

import org.snmp4j.security.UsmUser;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;

import java.util.ArrayList;
import java.util.List;

public class JsonUsmReaderTestBase {

    static final String USERS_JSON_PATH = "src/test/resources/users.json";
    static final String NOT_FOUND_USERS_JSON_PATH = "src/test/resources/not_found.json";
    static final String INVALID_USERS_JSON_PATH = "src/test/resources/invalid_users.json";

    static final List<UsmUser> expectedUsmUsers;

    static {
        expectedUsmUsers = new ArrayList<>();
        expectedUsmUsers.add(new UsmUser(
                new OctetString("user1"),
                new OID("1.3.6.1.6.3.10.1.1.2"),
                new OctetString("abc12345"),
                new OID("1.3.6.1.6.3.10.1.2.2"),
                new OctetString("abc12345")
        ));
        expectedUsmUsers.add(new UsmUser(
                new OctetString("user2"),
                new OID("1.3.6.1.6.3.10.1.1.3"),
                new OctetString("abc12345"),
                new OID("1.3.6.1.4.1.4976.2.2.1.1.2"),
                new OctetString("abc12345")
        ));
    }

}
