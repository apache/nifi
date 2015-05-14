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
package org.apache.nifi.web.api.dto.action.details;

import com.wordnik.swagger.annotations.ApiModelProperty;
import javax.xml.bind.annotation.XmlType;

/**
 * Details of the move operation.
 */
@XmlType(name = "moveDetails")
public class MoveDetailsDTO extends ActionDetailsDTO {

    private String previousGroupId;
    private String previousGroup;
    private String groupId;
    private String group;

    /**
     * @return id of the group the components previously belonged to
     */
    @ApiModelProperty(
            value = "The id of the group the components previously belonged to."
    )
    public String getPreviousGroupId() {
        return previousGroupId;
    }

    public void setPreviousGroupId(String previousGroupId) {
        this.previousGroupId = previousGroupId;
    }

    /**
     * @return name of the group of the components previously belonged to
     */
    @ApiModelProperty(
            value = "The name of the group the components previously belonged to."
    )
    public String getPreviousGroup() {
        return previousGroup;
    }

    public void setPreviousGroup(String previousGroup) {
        this.previousGroup = previousGroup;
    }

    /**
     * @return id of the group the components belong to
     */
    @ApiModelProperty(
            value = "The id of the group that components belong to."
    )
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * @return name of the group the components belong to
     */
    @ApiModelProperty(
            value = "The name of the group the components belong to."
    )
    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }
}
