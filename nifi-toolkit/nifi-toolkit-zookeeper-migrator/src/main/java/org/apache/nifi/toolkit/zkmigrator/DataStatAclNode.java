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

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.Objects;

public class DataStatAclNode {

    private final String path;
    private final byte[] data;
    private final Stat stat;
    private final List<ACL> acls;
    private final long ephemeralOwner;

    public DataStatAclNode(String path, byte[] data, Stat stat, List<ACL> acls, long ephemeralOwner) {
        this.path = Preconditions.checkNotNull(path, "path can not be null");
        this.data = data;
        this.stat = Preconditions.checkNotNull(stat, "stat can not be null");
        this.acls = acls == null ? ImmutableList.of() : ImmutableList.copyOf(acls);
        this.ephemeralOwner = ephemeralOwner;
    }

    public String getPath() {
        return path;
    }

    public byte[] getData() {
        return data;
    }

    public Stat getStat() {
        return stat;
    }

    public List<ACL> getAcls() {
        return acls;
    }

    public long getEphemeralOwner() {
        return ephemeralOwner;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataStatAclNode that = (DataStatAclNode) o;
        return Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("path", path)
                .add("acls", acls)
                .add("ephemeralOwner", ephemeralOwner)
                .toString();
    }
}
