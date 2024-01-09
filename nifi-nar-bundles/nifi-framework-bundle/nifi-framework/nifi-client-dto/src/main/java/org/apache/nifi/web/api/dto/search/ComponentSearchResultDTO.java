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
import java.util.List;

/**
 * The components that match a search performed on this NiFi.
 */
@XmlType(name = "componentSearchResult")
public class ComponentSearchResultDTO {

    private String id;
    private String groupId;
    private SearchResultGroupDTO parentGroup;
    private SearchResultGroupDTO versionedGroup;
    private String name;
    private List<String> matches;

    /**
     * @return id of the component that matched
     */
    @Schema(description = "The id of the component that matched the search."
    )
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return group id of the component that matched
     */
    @Schema(description = "The group id of the component that matched the search."
    )
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * @return parent group of the component that matched
     */
    @Schema(description = "The parent group of the component that matched the search."
    )
    public SearchResultGroupDTO getParentGroup() {
        return parentGroup;
    }

    public void setParentGroup(final SearchResultGroupDTO parentGroup) {
        this.parentGroup = parentGroup;
    }

    /**
     * @return the nearest versioned ancestor group of the component that matched
     */
    @Schema(description = "The nearest versioned ancestor group of the component that matched the search."
    )
    public SearchResultGroupDTO getVersionedGroup() {
        return versionedGroup;
    }

    public void setVersionedGroup(final SearchResultGroupDTO versionedGroup) {
        this.versionedGroup = versionedGroup;
    }

    /**
     * @return name of the component that matched
     */
    @Schema(description = "The name of the component that matched the search."
    )
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return What matched the search string for this component
     */
    @Schema(description = "What matched the search from the component."
    )
    public List<String> getMatches() {
        return matches;
    }

    public void setMatches(List<String> matches) {
        this.matches = matches;
    }

}
