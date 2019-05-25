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

class NssShellCommands implements ShellCommandsProvider {
    public String getUsersList() {
        return "getent passwd | cut -f 1,3,4 -d ':'";
    }

    public String getUserGroups() {
        return "id -nG %s | sed s/\\ /,/g";
    }

    // gets group name, group id, users
    public String getGroupsList() {
        return "getent group | cut -f 1,3 -d ':'";
    }

    // gets group members
    public String getGroupMembers() {
        return "getent group %s | cut -f 4 -d ':'";
    }

    public String getSystemCheck() {
        return "getent passwd"; // this gives exit code 0 on distros tested.
    }
}
