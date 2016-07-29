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
package org.apache.nifi.web.api.dto;

import com.wordnik.swagger.annotations.ApiModelProperty;
import javax.xml.bind.annotation.XmlType;

/**
 * Base class for all nifi components.
 */
@XmlType(name = "component")
public class ComponentDTO {

    private String id;

    private String parentGroupId;
    private PositionDTO position;

    /**
     * The id for this component.
     *
     * @return The id
     */
    @ApiModelProperty(
            value = "The id of the component."
    )
    public String getId() {
        return this.id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    /**
     * @return id for the parent group of this component if applicable, null otherwise
     */
    @ApiModelProperty(
            value = "The id of parent process group of this component if applicable."
    )
    public String getParentGroupId() {
        return parentGroupId;
    }

    public void setParentGroupId(String parentGroupId) {
        this.parentGroupId = parentGroupId;
    }

    /**
     * The position of this component in the UI if applicable, null otherwise.
     *
     * @return The position
     */
    @ApiModelProperty(
        value = "The position of this component in the UI if applicable."
    )
    public PositionDTO getPosition() {
        return position;
    }

    public void setPosition(final PositionDTO position) {
        this.position = position;
    }

    @Override
    public int hashCode() {
        return id == null ? 187 : 187 * id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != getClass()) {
            return false;
        }

        return id.equals(((ComponentDTO) obj).getId());
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + ":" + this.getId();
    }
}
