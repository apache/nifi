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
package org.apache.nifi.registry.db;

import org.apache.nifi.registry.security.key.Key;
import org.apache.nifi.registry.security.key.KeyService;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestDatabaseKeyService extends DatabaseBaseTest {

    @Autowired
    private KeyService keyService;

    @Test
    public void testGetKeyByIdWhenExists() {
        final Key existingKey = keyService.getKey("1");
        assertNotNull(existingKey);
        assertEquals("1", existingKey.getId());
        assertEquals("unit_test_tenant_identity", existingKey.getIdentity());
        assertEquals("0123456789abcdef", existingKey.getKey());
    }

    @Test
    public void testGetKeyByIdWhenDoesNotExist() {
        final Key existingKey = keyService.getKey("2");
        assertNull(existingKey);
    }

    @Test
    public void testGetOrCreateKeyWhenExists() {
        final Key existingKey = keyService.getOrCreateKey("unit_test_tenant_identity");
        assertNotNull(existingKey);
        assertEquals("1", existingKey.getId());
        assertEquals("unit_test_tenant_identity", existingKey.getIdentity());
        assertEquals("0123456789abcdef", existingKey.getKey());
    }

    @Test
    public void testGetOrCreateKeyWhenDoesNotExist() {
        final Key createdKey = keyService.getOrCreateKey("does-not-exist");
        assertNotNull(createdKey);
        assertNotNull(createdKey.getId());
        assertEquals("does-not-exist", createdKey.getIdentity());
        assertNotNull(createdKey.getKey());
    }

    @Test
    public void testDeleteKeyWhenExists() {
        final Key existingKey = keyService.getKey("1");
        assertNotNull(existingKey);

        keyService.deleteKey(existingKey.getIdentity());

        final Key deletedKey = keyService.getKey("1");
        assertNull(deletedKey);
    }
}
