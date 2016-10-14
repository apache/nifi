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
package org.apache.nifi.web.api.dto.action;

import com.wordnik.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.dto.action.component.details.ComponentDetailsDTO;
import org.apache.nifi.web.api.dto.action.details.ActionDetailsDTO;
import org.apache.nifi.web.api.dto.util.DateTimeAdapter;

import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Date;

/**
 * An action performed in this NiFi.
 */
@XmlType(name = "action")
public class ActionDTO {

    private Integer id;
    private String userIdentity;
    private Date timestamp;

    private String sourceId;
    private String sourceName;
    private String sourceType;
    private ComponentDetailsDTO componentDetails;

    private String operation;
    private ActionDetailsDTO actionDetails;

    /**
     * @return action id
     */
    @ApiModelProperty(
            value = "The action id."
    )
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    /**
     * @return user identity who perform this action
     */
    @ApiModelProperty(
            value = "The identity of the user that performed the action."
    )
    public String getUserIdentity() {
        return userIdentity;
    }

    public void setUserIdentity(String userIdentity) {
        this.userIdentity = userIdentity;
    }

    /**
     * @return action's timestamp
     */
    @XmlJavaTypeAdapter(DateTimeAdapter.class)
    @ApiModelProperty(
            value = "The timestamp of the action.",
            dataType = "string"
    )
    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * @return id of the source component of this action
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
     * @return name of the source component of this action
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
     * @return type of the source component of this action
     */
    @ApiModelProperty(
            value = "The type of the source component."
    )
    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    /**
     * @return component details (if any) for this action
     */
    @ApiModelProperty(
            value = "The details of the source component."
    )
    public ComponentDetailsDTO getComponentDetails() {
        return componentDetails;
    }

    public void setComponentDetails(ComponentDetailsDTO componentDetails) {
        this.componentDetails = componentDetails;
    }

    /**
     * @return operation being performed in this action
     */
    @ApiModelProperty(
            value = "The operation that was performed."
    )
    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    /**
     * @return action details (if any) for this action
     */
    @ApiModelProperty(
            value = "The details of the action."
    )
    public ActionDetailsDTO getActionDetails() {
        return actionDetails;
    }

    public void setActionDetails(ActionDetailsDTO actionDetails) {
        this.actionDetails = actionDetails;
    }

}
