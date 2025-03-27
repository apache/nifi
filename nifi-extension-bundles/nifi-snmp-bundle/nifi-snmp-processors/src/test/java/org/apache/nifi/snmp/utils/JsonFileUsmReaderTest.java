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
import org.junit.jupiter.api.Test;
import org.snmp4j.security.UsmUser;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonFileUsmReaderTest extends JsonUsmReaderTestBase {

    @Test
    void testReadJsonFile() {
        final UsmReader jsonFileUsmReader = new JsonFileUsmReader(USERS_JSON_PATH);
        final List<UsmUser> usmUsers = jsonFileUsmReader.readUsm();

        assertEquals(expectedUsmUsers, usmUsers);
    }

    @Test
    void testReadJsonFileThrowsException() {
        final UsmReader jsonFileUsmReader = new JsonFileUsmReader(NOT_FOUND_USERS_JSON_PATH);
        assertThrows(ProcessException.class, jsonFileUsmReader::readUsm);
    }

    @Test
    void testReadInvalidJsonThrowsException() {
        final UsmReader jsonFileUsmReader = new JsonFileUsmReader(INVALID_USERS_JSON_PATH);
        assertThrows(ProcessException.class, jsonFileUsmReader::readUsm);
    }

    @Test
    void testReadLegacyJsonThrowsException() {
        final UsmReader jsonFileUsmReader = new JsonFileUsmReader(LEGACY_USERS_JSON_PATH);
        assertThrows(ProcessException.class, jsonFileUsmReader::readUsm);
    }

}
