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


class RemoteShellCommands implements ShellCommandsProvider {
    // Carefully crafted command replacement string:
    private final static String remoteCommand = "ssh " +
        "-o 'StrictHostKeyChecking no' " +
        "-o 'PasswordAuthentication no' " +
        "-o \"RemoteCommand %s\" " +
        "-i %s -p %s -l root %s";

    private ShellCommandsProvider innerProvider;
    private String privateKeyPath;
    private String remoteHost;
    private Integer remotePort;

    private RemoteShellCommands() {
    }

    public static ShellCommandsProvider wrapOtherProvider(ShellCommandsProvider otherProvider, String keyPath, String host, Integer port) {
        RemoteShellCommands remote = new RemoteShellCommands();

        remote.innerProvider = otherProvider;
        remote.privateKeyPath = keyPath;
        remote.remoteHost = host;
        remote.remotePort = port;

        return remote;
    }

    public String getUsersList() {
        return String.format(remoteCommand, innerProvider.getUsersList(), privateKeyPath, remotePort, remoteHost);
    }

    public String getGroupsList() {
        return String.format(remoteCommand, innerProvider.getGroupsList(), privateKeyPath, remotePort, remoteHost);
    }

    public String getGroupMembers(String groupName) {
        return String.format(remoteCommand, innerProvider.getGroupMembers(groupName), privateKeyPath, remotePort, remoteHost);
    }

    public String getUserById(String userId) {
        return String.format(remoteCommand, innerProvider.getUserById(userId), privateKeyPath, remotePort, remoteHost);
    }

    public String getUserByName(String userName) {
        return String.format(remoteCommand, innerProvider.getUserByName(userName), privateKeyPath, remotePort, remoteHost);
    }

    public String getGroupById(String groupId) {
        return String.format(remoteCommand, innerProvider.getGroupById(groupId), privateKeyPath, remotePort, remoteHost);
    }

    public String getSystemCheck() {
        return String.format(remoteCommand, innerProvider.getSystemCheck(), privateKeyPath, remotePort, remoteHost);
    }
}
