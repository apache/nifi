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

import org.apache.nifi.processor.exception.ProcessException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.snmp4j.security.UsmUser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonUsmReaderTest extends JsonUsmReaderTestBase {

    private static String USERS_JSON;
    private static String INVALID_USERS_JSON;

    @BeforeAll
    public static void setup() throws IOException {
        USERS_JSON = readFile(USERS_JSON_PATH);
        INVALID_USERS_JSON = readFile(INVALID_USERS_JSON_PATH);
    }

    @Test
    void testReadJson() {
        final UsmReader jsonUsmReader = new JsonUsmReader(USERS_JSON);
        final List<UsmUser> usmUsers = jsonUsmReader.readUsm();

        assertEquals(expectedUsmUsers, usmUsers);
    }

    @Test
    void testReadInvalidJsonThrowsException() {
        final UsmReader jsonUsmReader = new JsonUsmReader(INVALID_USERS_JSON);
        assertThrows(ProcessException.class, jsonUsmReader::readUsm);
    }

    static String readFile(String path) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, StandardCharsets.UTF_8);
    }

}
