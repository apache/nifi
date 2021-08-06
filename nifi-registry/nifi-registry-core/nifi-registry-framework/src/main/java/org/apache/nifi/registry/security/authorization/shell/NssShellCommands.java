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
 * Provides shell commands to read users and groups on NSS-enabled systems.
 *
 * See `man 5 nsswitch.conf` for more info.
 */
class NssShellCommands implements ShellCommandsProvider {
    /**
     * @return Shell command string that will return a list of users.
     */
    public String getUsersList() {
        return "getent passwd | cut -f 1,3,4 -d ':'";
    }

    /**
     * @return Shell command string that will return a list of groups.
     */
    public String getGroupsList() {
        return "getent group | cut -f 1,3 -d ':'";
    }

    /**
     * @param groupName name of group.
     * @return Shell command string that will return a list of users for a group.
     */
    public String getGroupMembers(String groupName) {
        return String.format("getent group %s | cut -f 4 -d ':'", groupName);
    }

    /**
     * Gets the command for reading a single user by id.
     *
     * When executed, this command should output a single line, in the format used by `getUsersList`.
     *
     * @param userId name of user.
     * @return Shell command string that will read a single user.
     */
    @Override
    public String getUserById(String userId) {
        return String.format("getent passwd %s | cut -f 1,3,4 -d ':'", userId);
    }

    /**
     * This method reuses `getUserById` because the getent command is the same for
     * both uid and username.
     *
     * @param userName name of user.
     * @return Shell command string that will read a single user.
     */
    public String getUserByName(String userName) {
        return getUserById(userName);
    }

    /**
     * This method supports gid or group name because getent does.
     *
     * @param groupId name of group.
     * @return Shell command string that will read a single group.
     */
    public String getGroupById(String groupId) {
        return String.format("getent group %s | cut -f 1,3,4 -d ':'", groupId);
    }

    /**
     * This gives exit code 0 on all tested distributions.
     *
     * @return Shell command string that will exit normally (0) on a suitable system.
     */
    public String getSystemCheck() {
        return "getent --version";
    }
}