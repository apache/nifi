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
package org.apache.nifi.registry.security.authorization.shell;

/**
 * Common interface for shell command strings to read users and groups.
 *
 */
interface ShellCommandsProvider {
    /**
     * Gets the command for listing users.
     *
     * When executed, this command should output one record per line in this format:
     *
     * `username:user-id:primary-group-id`
     *
     * @return Shell command string that will return a list of users.
     */
    String getUsersList();

    /**
     * Gets the command for listing groups.
     *
     * When executed, this command should output one record per line in this format:
     *
     * `group-name:group-id`
     *
     * @return Shell command string that will return a list of groups.
     */
    String getGroupsList();

    /**
     * Gets the command for listing the members of a group.
     *
     * When executed, this command should output one line in this format:
     *
     * `user-name-1,user-name-2,user-name-n`
     *
     * @param groupName name of group.
     * @return Shell command string that will return a list of users for a group.
     */
    String getGroupMembers(String groupName);

    /**
     * Gets the command for reading a single user by id.  Implementations may return null if reading a single
     * user by id is not supported.
     *
     * When executed, this command should output a single line, in the format used by `getUsersList`.
     *
     * @param userId name of user.
     * @return Shell command string that will read a single user.
     */
    String getUserById(String userId);

    /**
     * Gets the command for reading a single user.  Implementations may return null if reading a single user by
     * username is not supported.
     *
     * When executed, this command should output a single line, in the format used by `getUsersList`.
     *
     * @param userName name of user.
     * @return Shell command string that will read a single user.
     */
    String getUserByName(String userName);

    /**
     * Gets the command for reading a single group.  Implementations may return null if reading a single group
     * by name is not supported.
     *
     * When executed, this command should output a single line, in the format used by `getGroupsList`.
     *
     * @param groupId name of group.
     * @return Shell command string that will read a single group.
     */
    String getGroupById(String groupId);

    /**
     * Gets the command for checking the suitability of the host system.
     *
     * The command is expected to exit with status 0 (zero) to indicate success, and any other status
     * to indicate failure.
     *
     * @return Shell command string that will exit normally (0) on a suitable system.
     */
    String getSystemCheck();
}
