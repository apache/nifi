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

import com.wordnik.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.dto.RevisionDTO;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Map;

/**
 * A serialized representation of this class can be placed in the entity body of a request to the API.
 */
@XmlRootElement(name = "updateControllerServiceReferenceRequestEntity")
public class UpdateControllerServiceReferenceRequestEntity extends Entity {

    private String id;
    private String state;
    private Map<String, RevisionDTO> referencingComponentRevisions;

    @ApiModelProperty(
        value = "The identifier of the Controller Service."
    )
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @ApiModelProperty(
        value = "The new state of the references for the controller service.",
        allowableValues = "ENABLED, DISABLED, RUNNING, STOPPED"
    )
    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    @ApiModelProperty(
        value = "The revisions for all referencing components."
    )
    public Map<String, RevisionDTO> getReferencingComponentRevisions() {
        return referencingComponentRevisions;
    }

    public void setReferencingComponentRevisions(Map<String, RevisionDTO> referencingComponentRevisions) {
        this.referencingComponentRevisions = referencingComponentRevisions;
    }
}
