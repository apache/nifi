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
package org.apache.nifi.web.dao;

import org.apache.nifi.authorization.User;
import org.apache.nifi.web.api.dto.UserDTO;

import java.util.Set;

public interface UserDAO {

    /**
     * @param userId user ID
     * @return Determines if the specified user exists
     */
    boolean hasUser(String userId);

    /**
     * Creates a user.
     *
     * @param userDTO The user DTO
     * @return The user transfer object
     */
    User createUser(UserDTO userDTO);

    /**
     * Gets the user with the specified ID.
     *
     * @param userId The user ID
     * @return The user transfer object
     */
    User getUser(String userId);

    /**
     * Gets all users.
     *
     * @return The user transfer objects
     */
    Set<User> getUsers();

    /**
     * Updates the specified user.
     *
     * @param userDTO The user DTO
     * @return The user transfer object
     */
    User updateUser(UserDTO userDTO);

    /**
     * Deletes the specified user.
     *
     * @param userId The user ID
     * @return The user transfer object of the deleted user
     */
    User deleteUser(String userId);

}
