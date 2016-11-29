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
import org.apache.nifi.web.api.dto.util.TimeAdapter;

import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Date;

/**
 * A bulletin that represents a notification about a passing event including, the source component (if applicable), the timestamp, the message, and where the bulletin originated (if applicable).
 */
@XmlType(name = "bulletin")
public class BulletinDTO {

    private Long id;
    private String nodeAddress;
    private String category;
    private String groupId;
    private String sourceId;
    private String sourceName;
    private String level;
    private String message;
    private Date timestamp;

    /**
     * @return id of this message
     */
    @ApiModelProperty(
            value = "The id of the bulletin."
    )
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    /**
     * @return When clustered, the address of the node from which this bulletin originated
     */
    @ApiModelProperty(
            value = "If clustered, the address of the node from which the bulletin originated."
    )
    public String getNodeAddress() {
        return nodeAddress;
    }

    public void setNodeAddress(String nodeAddress) {
        this.nodeAddress = nodeAddress;
    }

    /**
     * @return group id of the source component
     */
    @ApiModelProperty(
            value = "The group id of the source component."
    )
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * @return category of this message
     */
    @ApiModelProperty(
            value = "The category of this bulletin."
    )
    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    /**
     * @return actual message
     */
    @ApiModelProperty(
            value = "The bulletin message."
    )
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * @return id of the source of this message
     */
    @ApiModelProperty(
            value = "The id of the source component."
    )
    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    /**
     * @return name of the source of this message
     */
    @ApiModelProperty(
            value = "The name of the source component."
    )
    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    /**
     * @return level of this bulletin
     */
    @ApiModelProperty(
            value = "The level of the bulletin."
    )
    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    /**
     * @return When this bulletin was generated as a formatted string
     */
    @XmlJavaTypeAdapter(TimeAdapter.class)
    @ApiModelProperty(
            value = "When this bulletin was generated.",
            dataType = "string"
    )
    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

}
