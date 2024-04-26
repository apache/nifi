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
package org.apache.nifi.redis.testcontainers;

import org.springframework.lang.NonNull;
import org.testcontainers.utility.DockerImageName;

public class RedisReplicaContainer extends RedisContainer {

    public RedisReplicaContainer(@NonNull DockerImageName dockerImageName) {
        super(dockerImageName);
    }

    public RedisReplicaContainer(@NonNull String fullImageName) {
        this(DockerImageName.parse(fullImageName));
    }

    @NonNull
    protected String masterHost = "localhost";
    protected int masterPort = REDIS_PORT;

    public void setMasterHost(@NonNull String masterHost) {
        this.masterHost = masterHost;
    }

    public void setMasterPort(int masterPort) {
        this.masterPort = masterPort;
    }

    public void setReplicaOf(@NonNull String masterHost, int masterPort) {
        setMasterHost(masterHost);
        setMasterPort(masterPort);
    }

    @Override
    protected void adjustConfiguration() {
        addConfigurationOption("port " + port);

        if (username != null) {
            final String userPassword = password == null ? "nopass" : ">" + password;
            addConfigurationOption("user " + username + " on " + userPassword + " ~* allcommands allchannels");
        }

        if (password != null) {
            addConfigurationOption("requirepass " + password);
            addConfigurationOption("masterauth " + password);
        }

        addConfigurationOption("replicaof " + masterHost + " " + masterPort);
    }
}
