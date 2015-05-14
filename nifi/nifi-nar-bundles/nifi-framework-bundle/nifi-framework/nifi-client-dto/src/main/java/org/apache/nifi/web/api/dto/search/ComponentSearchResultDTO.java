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

import com.wordnik.swagger.annotations.ApiModelProperty;
import java.util.List;
import javax.xml.bind.annotation.XmlType;

/**
 * The components that match a search performed on this NiFi.
 */
@XmlType(name = "componentSearchResult")
public class ComponentSearchResultDTO {

    private String id;
    private String groupId;
    private String name;
    private List<String> matches;

    /**
     * @return id of the component that matched
     */
    @ApiModelProperty(
            value = "The id of the component that matched the search."
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
    @ApiModelProperty(
            value = "The group id of the component that matched the search."
    )
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * @return name of the component that matched
     */
    @ApiModelProperty(
            value = "The name of the component that matched the search."
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
    @ApiModelProperty(
            value = "What matched the search from the component."
    )
    public List<String> getMatches() {
        return matches;
    }

    public void setMatches(List<String> matches) {
        this.matches = matches;
    }

}
