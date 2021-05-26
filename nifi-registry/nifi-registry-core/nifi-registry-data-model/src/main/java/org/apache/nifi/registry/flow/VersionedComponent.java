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

import java.util.Objects;

import io.swagger.annotations.ApiModelProperty;


public abstract class VersionedComponent {

    private String identifier;
    private String groupId;
    private String name;
    private String comments;
    private Position position;

    @ApiModelProperty("The component's unique identifier")
    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    @ApiModelProperty("The ID of the Process Group that this component belongs to")
    public String getGroupIdentifier() {
        return groupId;
    }

    public void setGroupIdentifier(String groupId) {
        this.groupId = groupId;
    }

    @ApiModelProperty("The component's name")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @ApiModelProperty("The component's position on the graph")
    public Position getPosition() {
        return position;
    }

    public void setPosition(Position position) {
        this.position = position;
    }

    @ApiModelProperty("The user-supplied comments for the component")
    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    public abstract ComponentType getComponentType();

    public void setComponentType(ComponentType type) {
        // purposely do nothing here, this just to allow unmarshalling
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof VersionedComponent)) {
            return false;
        }
        final VersionedComponent other = (VersionedComponent) obj;
        return Objects.equals(identifier, other.identifier);
    }
}
