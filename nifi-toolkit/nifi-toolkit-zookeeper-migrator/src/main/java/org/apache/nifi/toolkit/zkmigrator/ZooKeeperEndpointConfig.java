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
package org.apache.nifi.toolkit.zkmigrator;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

class ZooKeeperEndpointConfig {
    private final String connectString;
    private final List<String> servers;
    private final String path;

    ZooKeeperEndpointConfig(String connectString) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(connectString), "connectString can not be null or empty");
        this.connectString = connectString;

        final String[] connectStringPath = connectString.split("/", 2);
        this.servers = Lists.newArrayList(connectStringPath[0].split(","));
        if (connectStringPath.length == 2) {
            this.path = '/' + Joiner.on('/').join(Splitter.on('/').omitEmptyStrings().trimResults().split(connectStringPath[1]));
        } else {
            path = "/";
        }
    }

    public String getConnectString() {
        return connectString;
    }

    public List getServers() {
        return servers;
    }

    public String getPath() {
        return path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ZooKeeperEndpointConfig that = (ZooKeeperEndpointConfig) o;
        return Objects.equals(connectString, that.connectString)
                && Objects.equals(servers, that.servers)
                && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectString, servers, path);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("connectString", connectString)
                .add("servers", servers)
                .add("path", path)
                .toString();
    }
}
