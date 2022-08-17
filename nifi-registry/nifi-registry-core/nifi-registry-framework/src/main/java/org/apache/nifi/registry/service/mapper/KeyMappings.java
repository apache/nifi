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
package org.apache.nifi.registry.service.mapper;

import org.apache.nifi.registry.db.entity.KeyEntity;
import org.apache.nifi.registry.security.key.Key;

/**
 * Mappings between key DB entities and data model.
 */
public class KeyMappings {

    public static Key map(final KeyEntity keyEntity) {
        final Key key = new Key();
        key.setId(keyEntity.getId());
        key.setIdentity(keyEntity.getTenantIdentity());
        key.setKey(keyEntity.getKeyValue());
        return key;
    }

    public static KeyEntity map(final Key key) {
        final KeyEntity keyEntity = new KeyEntity();
        keyEntity.setId(key.getId());
        keyEntity.setTenantIdentity(key.getIdentity());
        keyEntity.setKeyValue(key.getKey());
        return keyEntity;
    }

}
