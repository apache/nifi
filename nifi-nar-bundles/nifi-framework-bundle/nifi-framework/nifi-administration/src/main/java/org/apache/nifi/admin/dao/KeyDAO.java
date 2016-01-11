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
package org.apache.nifi.admin.dao;

import org.apache.nifi.key.Key;

/**
 * Key data access.
 */
public interface KeyDAO {

    /**
     * Gets the key for the specified user identity. Returns null if no key exists for the key id.
     *
     * @param id The key id
     * @return The key or null
     */
    Key findKeyById(int id);

    /**
     * Gets the latest key for the specified identity. Returns null if no key exists for the user identity.
     *
     * @param identity The identity
     * @return The key or null
     */
    Key findLatestKeyByIdentity(String identity);

    /**
     * Creates a key for the specified user identity.
     *
     * @param identity The user identity
     * @return The key
     */
    Key createKey(String identity);

    /**
     * Deletes all keys for the specified user identity.
     *
     * @param identity The user identity
     */
    void deleteKeys(String identity);
}
