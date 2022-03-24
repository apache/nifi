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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

class ZooKeeperNode {

    private final String path;
    private final List<ZooKeeperNode> children;

    public ZooKeeperNode(String path, List<ZooKeeperNode> children) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path), "path can not be null or empty");
        this.path = path;
        this.children = children == null ? ImmutableList.of() : ImmutableList.copyOf(children);
    }

    public String getPath() {
        return path;
    }

    public List<ZooKeeperNode> getChildren() {
        return children;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ZooKeeperNode node = (ZooKeeperNode) o;
        return Objects.equals(path, node.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("path", path)
                .add("children", children)
                .toString();
    }
}
