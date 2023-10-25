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
package org.apache.nifi.processors.standard.util;

import org.apache.commons.io.FileUtils;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.sftp.server.SftpSubsystemFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class SSHTestServer {
    private static SshServer sshd;

    private String virtualFileSystemPath = "target/ssh_vfs/";

    private final String host = "127.0.0.1";

    private String username = "nifiuser";

    private String password = "nifipassword";

    public int getSSHPort(){
        return sshd.getPort();
    }

    public String getVirtualFileSystemPath() {
        return virtualFileSystemPath;
    }

    public void setVirtualFileSystemPath(String virtualFileSystemPath) {
        this.virtualFileSystemPath = virtualFileSystemPath;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public void startServer() throws IOException {
        sshd = SshServer.setUpDefaultServer();
        sshd.setHost(host);

        sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());

        //Accept all keys for authentication
        sshd.setPublickeyAuthenticator((s, publicKey, serverSession) -> true);

        //Allow username/password authentication using pre-defined credentials
        sshd.setPasswordAuthenticator((username, password, serverSession) ->  this.username.equals(username) && this.password.equals(password));

        //Setup Virtual File System (VFS)
        //Ensure VFS folder exists
        Path dir = Paths.get(getVirtualFileSystemPath());
        Files.createDirectories(dir);
        sshd.setFileSystemFactory(new VirtualFileSystemFactory(dir.toAbsolutePath()));

        List<SftpSubsystemFactory> sftpCommandFactory = new ArrayList<>();
        sftpCommandFactory.add(new SftpSubsystemFactory());
        sshd.setSubsystemFactories(sftpCommandFactory);

        sshd.start();
    }

    public void stopServer() throws IOException {
        if (sshd == null) {
            return;
        }
        sshd.stop(true);

        Path dir = Paths.get(getVirtualFileSystemPath());
        FileUtils.deleteDirectory(dir.toFile());
    }
}