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

import java.util.Objects;

class ZooKeeperEndpointConfig {
    private final String connectString;
    private final String path;

    ZooKeeperEndpointConfig(String connectString, String path) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(connectString), "connectString can not be null or empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path), "path can not be null or empty");
        this.connectString = connectString;
        this.path = '/' + Joiner.on('/').join(Splitter.on('/').omitEmptyStrings().trimResults().split(path));
    }

    public String getConnectString() {
        return connectString;
    }

    public String getPath() {
        return path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ZooKeeperEndpointConfig that = (ZooKeeperEndpointConfig) o;
        return Objects.equals(connectString, that.connectString) && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectString, path);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("connectString", connectString)
                .add("path", path)
                .toString();
    }
}
