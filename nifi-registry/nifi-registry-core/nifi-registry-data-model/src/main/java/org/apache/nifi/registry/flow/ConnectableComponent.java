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

package org.apache.nifi.registry.flow;

import io.swagger.annotations.ApiModelProperty;
import java.util.Objects;


public class ConnectableComponent {
    private String id;
    private ConnectableComponentType type;
    private String groupId;
    private String name;
    private String comments;

    @ApiModelProperty(value = "The id of the connectable component.", required = true)
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @ApiModelProperty(value = "The type of component the connectable is.", required = true)
    public ConnectableComponentType getType() {
        return type;
    }

    public void setType(ConnectableComponentType type) {
        this.type = type;
    }

    @ApiModelProperty(value = "The id of the group that the connectable component resides in", required = true)
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @ApiModelProperty("The name of the connectable component")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @ApiModelProperty("The comments for the connectable component.")
    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, groupId, name, type, comments);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof ConnectableComponent)) {
            return false;
        }
        final ConnectableComponent other = (ConnectableComponent) obj;
        return Objects.equals(id, other.id);
    }
}
