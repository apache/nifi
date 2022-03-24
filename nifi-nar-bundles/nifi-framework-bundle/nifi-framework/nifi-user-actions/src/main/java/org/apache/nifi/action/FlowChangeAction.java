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
package org.apache.nifi.action;

import org.apache.nifi.action.component.details.ComponentDetails;
import org.apache.nifi.action.details.ActionDetails;
import java.util.Date;

/**
 *
 */
public class FlowChangeAction implements Action {

    private Integer id;
    private String userIdentity;
    private Date timestamp;

    private String sourceId;
    private String sourceName;
    private Component sourceType;
    private ComponentDetails componentDetails;

    private Operation operation;
    private ActionDetails actionDetails;

    @Override
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    @Override
    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String getUserIdentity() {
        return userIdentity;
    }

    public void setUserIdentity(String userIdentity) {
        this.userIdentity = userIdentity;
    }

    @Override
    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    @Override
    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    @Override
    public Component getSourceType() {
        return sourceType;
    }

    public void setSourceType(Component sourceType) {
        this.sourceType = sourceType;
    }

    @Override
    public ComponentDetails getComponentDetails() {
        return componentDetails;
    }

    public void setComponentDetails(ComponentDetails componentDetails) {
        this.componentDetails = componentDetails;
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    @Override
    public ActionDetails getActionDetails() {
        return actionDetails;
    }

    public void setActionDetails(ActionDetails actionDetails) {
        this.actionDetails = actionDetails;
    }
}
