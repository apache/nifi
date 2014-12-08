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

import java.util.Set;
import org.apache.nifi.authorization.Authority;

/**
 * Authority data access.
 */
public interface AuthorityDAO {

    /**
     * Finds all Authority for the specified user.
     *
     * @param userId
     * @return
     */
    Set<Authority> findAuthoritiesByUserId(String userId) throws DataAccessException;

    /**
     * Creates a new Authorities for the specified user.
     *
     * @param authorities
     * @param userId
     */
    void createAuthorities(Set<Authority> authorities, String userId) throws DataAccessException;

    /**
     * Removes all Authorities for the specified user.
     *
     * @param userId
     * @throws DataAccessException
     */
    void deleteAuthorities(String userId) throws DataAccessException;

    /**
     * Removes the specified Authority.
     *
     * @param authorities
     * @param userId
     */
    void deleteAuthorities(Set<Authority> authorities, String userId) throws DataAccessException;
}
