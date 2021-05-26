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
package org.apache.nifi.registry.revision.entity;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(description = "The revision information for an entity managed through the REST API.")
public class RevisionInfo {

    private String clientId;
    private Long version;
    private String lastModifier;

    public RevisionInfo() {
    }

    public RevisionInfo(String clientId, Long version) {
        this(clientId, version, null);
    }

    public RevisionInfo(String clientId, Long version, String lastModifier) {
        this.clientId = clientId;
        this.version = version;
        this.lastModifier = lastModifier;
    }

    @ApiModelProperty(
            value = "A client identifier used to make a request. By including a client identifier, the API can allow multiple requests " +
                    "without needing the current revision. Due to the asynchronous nature of requests/responses this was implemented to " +
                    "allow the client to make numerous requests without having to wait for the previous response to come back."
    )
    public String getClientId() {
        return clientId;
    }

    public void setClientId(final String clientId) {
        this.clientId = clientId;
    }

    @ApiModelProperty(
            value = "NiFi Registry employs an optimistic locking strategy where the client must include a revision in their request " +
                    "when performing an update. In a response to a mutable flow request, this field represents the updated base version."
    )
    public Long getVersion() {
        return version;
    }

    public void setVersion(final Long version) {
        this.version = version;
    }

    @ApiModelProperty(
            value = "The user that last modified the entity.",
            readOnly = true
    )
    public String getLastModifier() {
        return lastModifier;
    }

    public void setLastModifier(final String lastModifier) {
        this.lastModifier = lastModifier;
    }

}
