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
package org.apache.nifi.web.security.jwt.key.service;

import java.security.Key;
import java.time.Instant;
import java.util.Optional;

/**
 * Verification Key Service for storing and retrieving keys
 */
public interface VerificationKeyService {
    /**
     * Find Key using specified Key Identifier
     *
     * @param id Key Identifier
     * @return Optional Key
     */
    Optional<Key> findById(String id);

    /**
     * Delete Expired Keys
     */
    void deleteExpired();

    /**
     * Save Key with associated expiration
     *
     * @param id Key Identifier
     * @param key Key
     * @param expiration Expiration
     */
    void save(String id, Key key, Instant expiration);

    /**
     * Set Expiration for specified Key Identifier
     *
     * @param id Key Identifier
     * @param expiration Expiration
     */
    void setExpiration(String id, Instant expiration);
}
