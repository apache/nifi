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
package org.apache.nifi.web.api.entity;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.wordnik.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.dto.ReadablePermission;
import org.apache.nifi.web.api.dto.action.ActionDTO;
import org.apache.nifi.web.api.dto.util.DateTimeAdapter;

import java.util.Date;

/**
 * A serialized representation of this class can be placed in the entity body of a request or response to or from the API. This particular entity holds a reference to an ActionDTO.
 */
@XmlRootElement(name = "actionEntity")
public class ActionEntity extends Entity implements ReadablePermission {

    private Integer id;
    private Date timestamp;
    private String sourceId;
    private Boolean canRead;

    private ActionDTO action;

    /**
     * The ActionDTO that is being serialized.
     *
     * @return The ActionDTO object
     */
    public ActionDTO getAction() {
        return action;
    }

    public void setAction(ActionDTO action) {
        this.action = action;
    }

    /**
     * @return The action ID.
     */
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    /**
     * @return The timestamp of the action.
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
     * @return The id of the source component.
     */
    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    @Override
    public Boolean getCanRead() {
        return canRead;
    }

    @Override
    public void setCanRead(Boolean canRead) {
        this.canRead = canRead;
    }
}
