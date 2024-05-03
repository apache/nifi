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
package org.apache.nifi.web.api.dto.search;

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.xml.bind.annotation.XmlType;

/**
 * The result's group level of a performed search.
 */
@XmlType(name = "searchResultGroup")
public class SearchResultGroupDTO {
    private String id;
    private String name;

    /**
     * @return id of this group
     */
    @Schema(description = "The id of the group.",
            requiredMode = Schema.RequiredMode.REQUIRED
    )
    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    /**
     * @return name of this group
     */
    @Schema(description = "The name of the group."
    )
    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }
}
