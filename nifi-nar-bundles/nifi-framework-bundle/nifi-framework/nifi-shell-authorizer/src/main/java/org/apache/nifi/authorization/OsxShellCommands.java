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
package org.apache.nifi.authorization;


/**
 * Provides shell commands to read users and groups on Mac OSX systems.
 *
 * See `man dscl` for more info.
 */
class OsxShellCommands implements ShellCommandsProvider {
    /**
     * @return Shell command string that will return a list of users.
     */
    public String getUsersList() {
        return "dscl . -readall /Users UniqueID PrimaryGroupID | awk 'BEGIN { OFS = \":\"; ORS=\"\\n\"; i=0;} /RecordName: / {name = $2;i = 0;}" +
               "/PrimaryGroupID: / {gid = $2;} /^ / {if (i == 0) { i++; name = $1;}} /UniqueID: / {uid = $2;print name, uid, gid;}' | grep -v ^_";
    }

    /**
     * @return Shell command string that will return a list of groups.
     */
    public String getGroupsList() {
        return "dscl . -list /Groups PrimaryGroupID  | grep -v '^_' | sed 's/ \\{1,\\}/:/g'";
    }

    /**
     *
     * @param groupName name of group.
     * @return Shell command string that will return a list of users for a group.
     */
    public String getGroupMembers(String groupName) {
        return String.format("dscl . -read /Groups/%s GroupMembership | cut -f 2- -d ' ' | sed 's/\\ /,/g'", groupName);
    }

    /**
     * @param userId name of user.
     * @return Shell command string that will read a single user.
     */
    @Override
    public String getUserById(String userId) {
        return String.format("id -P %s | cut -f 1,3,4 -d ':'", userId);
    }

    /**
     * @param userName name of user.
     * @return Shell command string that will read a single user.
     */
    public String getUserByName(String userName) {
        return getUserById(userName); // 'id' command works for both uid/username
    }

    /**
     * @param groupId name of group.
     * @return Shell command string that will read a single group.
     */
    public String getGroupById(String groupId) {
        return String.format(" dscl . -read /Groups/`dscl . -search /Groups gid %s | head -n 1 | cut -f 1` RecordName PrimaryGroupID | awk 'BEGIN { OFS = \":\"; ORS=\"\\n\"; i=0;} " +
                             "/RecordName: / {name = $2;i = 1;}/PrimaryGroupID: / {gid = $2;}; {if (i==1) {print name,gid,\"\"}}'", groupId);
    }

    /**
     * @return Shell command string that will exit normally (0) on a suitable system.
     */
    public String getSystemCheck() {
        return "which dscl";
    }
}
