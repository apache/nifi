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

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.xml.bind.annotation.XmlType;

/**
 * Details for the access configuration.
 */
@XmlType(name = "accessPolicy")
public class ComponentReferenceDTO extends ComponentDTO {

    private String id;
    private String parentGroupId;
    private String name;

    /**
     * The id for this component.
     *
     * @return The id
     */
    @Schema(description = "The id of the component."
    )
    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public void setId(final String id) {
        this.id = id;
    }

    /**
     * @return id for the parent group of this component if applicable, null otherwise
     */
    @Schema(description = "The id of parent process group of this component if applicable."
    )
    @Override
    public String getParentGroupId() {
        return parentGroupId;
    }

    @Override
    public void setParentGroupId(String parentGroupId) {
        this.parentGroupId = parentGroupId;
    }

    /**
     * @return id for the parent group of this component if applicable, null otherwise
     */
    @Schema(description = "The name of the component."
    )
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
