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
import org.apache.nifi.web.api.dto.AccessPolicyDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * A base type for request/response entities.
 */
@XmlRootElement(name = "entity")
public class ComponentEntity extends Entity {

    private RevisionDTO revision;
    private String id;
    private String uri;
    private PositionDTO position;
    private AccessPolicyDTO accessPolicy;

    /**
     * @return revision for this request/response
     */
    @ApiModelProperty(
            value = "The revision for this request/response. The revision is required for any mutable flow requests and is included in all responses."
    )
    public RevisionDTO getRevision() {
        if (revision == null) {
            return new RevisionDTO();
        } else {
            return revision;
        }
    }

    public void setRevision(RevisionDTO revision) {
        this.revision = revision;
    }

    /**
     * The id for this component.
     *
     * @return The id
     */
    @ApiModelProperty(
        value = "The id of the component."
    )
    public String getId() {
        return this.id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    /**
     * The uri for linking to this component in this NiFi.
     *
     * @return The uri
     */
    @ApiModelProperty(
        value = "The URI for futures requests to the component."
    )
    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    /**
     * The position of this component in the UI if applicable, null otherwise.
     *
     * @return The position
     */
    @ApiModelProperty(
        value = "The position of this component in the UI if applicable."
    )
    public PositionDTO getPosition() {
        return position;
    }

    public void setPosition(PositionDTO position) {
        this.position = position;
    }

    /**
     * The access policy for this component.
     *
     * @return The access policy
     */
    @ApiModelProperty(
        value = "The access policy for this component."
    )
    public AccessPolicyDTO getAccessPolicy() {
        return accessPolicy;
    }

    public void setAccessPolicy(AccessPolicyDTO accessPolicy) {
        this.accessPolicy = accessPolicy;
    }
}
